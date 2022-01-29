package ingest

import (
	"context"
	"encoding/hex"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"github.com/viderstv/common/streaming/av"
	"github.com/viderstv/common/streaming/protocol/rtmp"
	"github.com/viderstv/common/streaming/protocol/rtmp/core"
	"github.com/viderstv/common/streaming/protocol/rtmp/handler"
	"github.com/viderstv/common/structures"
	"github.com/viderstv/common/svc/mongo"
	"github.com/viderstv/common/utils"
	"github.com/viderstv/common/utils/ffprobe"
	"github.com/viderstv/ingest/src/global"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(gCtx global.Context) <-chan struct{} {
	done := make(chan struct{})

	handler := handler.New()

	mp := map[string]*Stream{}
	idMp := map[string]string{}
	mtx := sync.Mutex{}

	server := rtmp.New(rtmp.Config{
		Logger: logrus.New(),
		OnError: func(err error) {
			logrus.Error("error in rtmp: ", err)
		},
		OnNewStream: func(addr net.Addr) bool {
			logrus.Info(addr)
			return true
		},
		OnStreamClose: func(info av.Info, addr net.Addr) {
			if info.Publisher {
				defer handler.StopStream(info.Key)
			}
			mtx.Lock()
			if stream, ok := mp[info.ID]; ok {
				if stream.shutdown {
					logrus.Info("clean shutdown: ")
				} else {
					if err := gCtx.Inst().Redis.SetEX(gCtx, fmt.Sprintf("unclean-shutdown:%s", stream.userID.Hex()), stream.streamID.Hex(), time.Second*90); err != nil {
						logrus.Error("failed to set redis key: ", err)
					}
				}
				if _, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameStreams).UpdateOne(gCtx, bson.M{
					"_id":      stream.streamID,
					"revision": stream.revision,
				}, bson.M{
					"$set": bson.M{
						"ended_at": time.Now(),
					},
				}); err != nil {
					logrus.Error("failed to update mongo: ", err)
				}
				delete(mp, info.ID)
				if idMp[stream.streamID.Hex()] == info.ID {
					delete(idMp, stream.streamID.Hex())
				}
			}
			mtx.Unlock()
		},
		AuthStream: func(info *av.Info, addr net.Addr) bool {
			localLog := logrus.WithField("info", info.String()).WithField("addr", addr.String())

			start := time.Now()
			if !info.Publisher {
				if info.App == "internal" {
					pl := structures.JwtInternalRead{}
					data, err := hex.DecodeString(info.Name)
					if err != nil {
						localLog.Error("failed to decode: ", err)
						return false
					}

					if err := structures.DecodeJwt(&pl, gCtx.Config().RTMP.Auth.JwtToken, utils.B2S(data)); err != nil {
						localLog.Error("failed to decode: ", err)
						return false
					}

					mtx.Lock()
					defer mtx.Unlock()
					if key, ok := idMp[pl.StreamID.Hex()]; !ok {
						localLog.Debugf("stream not in map: %s %v", pl.StreamID.Hex(), idMp)
						return false
					} else {
						info.Key = key
					}

					return true
				}

				return false
			}

			if !strings.HasPrefix(info.Name, "live_") || info.App != "live" {
				return false
			}

			splits := strings.SplitN(strings.TrimPrefix(info.Name, "live_"), "_", 2)
			uID, err := primitive.ObjectIDFromHex(splits[0])
			if err != nil {
				return false
			}

			ctx, cancel := context.WithTimeout(gCtx, time.Second*5)
			defer cancel()

			res := gCtx.Inst().Mongo.Collection(mongo.CollectionNameUsers).FindOne(ctx, bson.M{
				"_id":                uID,
				"channel.stream_key": splits[1],
			})
			user := structures.User{}
			err = res.Err()
			if err == nil {
				err = res.Decode(&user)
			}
			if err != nil {
				if err != mongo.ErrNoDocuments {
					localLog.Error("failed to query user: ", err)
				}
				return false
			}
			if user.Role < structures.GlobalRoleStreamer {
				return false
			}

			var sID primitive.ObjectID
			_sID, err := gCtx.Inst().Redis.Get(gCtx, fmt.Sprintf("unclean-shutdown:%s", uID.Hex()))
			if err != nil && err != redis.Nil {
				localLog.Error("failed to query redis: ", err)
			} else if err != redis.Nil {
				_ = gCtx.Inst().Redis.Del(gCtx, fmt.Sprintf("unclean-shutdown:%s", uID.Hex()))
				sID, err = primitive.ObjectIDFromHex(_sID.(string))
				if err != nil {
					localLog.Error("invalid entry in redis: ", err)
				}
			}

			revision := int32(0)

			if !sID.IsZero() {
				res := gCtx.Inst().Mongo.Collection(mongo.CollectionNameStreams).FindOneAndUpdate(gCtx, bson.M{
					"_id": sID,
				}, bson.M{
					"$set": bson.M{
						"ended_at": time.Time{},
					},
					"$inc": bson.M{
						"revision": int32(1),
					},
				})
				stream := structures.Stream{}
				err = res.Err()
				if err == nil {
					err = res.Decode(&stream)
				}
				if err != nil {
					localLog.Error("failed to fetch old stream: ", err)
					sID = primitive.NilObjectID
				} else {
					revision = stream.Revision + 1
				}
			}

			if sID.IsZero() {
				// new stream
				sID = primitive.NewObjectIDFromTimestamp(start)
				_, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameStreams).InsertOne(gCtx, structures.Stream{
					ID:        sID,
					UserID:    uID,
					Title:     user.Channel.Title,
					StartedAt: start,
					Revision:  revision,
				})
				if err != nil {
					localLog.Error("failed to insert new stream: ", err)
					return false
				}
			}

			if _, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameUsers).UpdateOne(gCtx, bson.M{"_id": uID}, bson.M{
				"$set": bson.M{
					"channel.last_live": start,
				},
			}); err != nil {
				localLog.Error("failed to update last live: ", err)
			}

			mtx.Lock()
			mp[info.ID] = &Stream{
				info:     *info,
				userID:   uID,
				streamID: sID,
				revision: revision,
			}
			idMp[sID.Hex()] = info.ID
			mtx.Unlock()

			return true
		},
		HandleCmdChunk: func(info av.Info, vs []interface{}, chunk *core.ChunkStream) error {
			if len(vs) != 0 && vs[0] == "FCUnpublish" {
				mtx.Lock()
				if stream, ok := mp[info.ID]; ok {
					stream.shutdown = true
				}
				mtx.Unlock()
			}

			return nil
		},
		HandlePublisher: func(info av.Info, reader av.ReadCloser) {
			localLog := logrus.WithField("info", info.String())
			var statsFn func() rtmp.Stats

			if vir, ok := reader.(*rtmp.VirReader); ok {
				statsFn = vir.Stats
			} else {
				statsFn = func() rtmp.Stats {
					return rtmp.Stats{}
				}
			}

			mtx.Lock()
			stream := mp[info.Key]
			mtx.Unlock()
			ctx, cancel := context.WithCancel(gCtx)
			defer cancel()
			sub := make(chan string, 10)
			defer close(sub)

			{
				gCtx.Inst().Redis.Subscribe(ctx, sub, fmt.Sprintf("user-live:%s", stream.userID))
				go func() {
					e := structures.RedisRtmpEvent{}
					for evt := range sub {
						if err := json.UnmarshalFromString(evt, &e); err != nil {
							localLog.Warn("bad event from redis: ", evt)
							continue
						}

						switch e.Type {
						case structures.RedisRtmpEventTypeKill:
							if e.Key != info.Key {
								stream.shutdown = true
								reader.Close()
								return
							}
						default:
							localLog.Warn("unsupported rtmp redis event: ", e.Type)
						}
					}
				}()

				content, _ := json.MarshalToString(structures.RedisRtmpEvent{
					Type: structures.RedisRtmpEventTypeKill,
					Key:  info.Key,
				})
				if err := gCtx.Inst().Redis.Publish(ctx, fmt.Sprintf("user-live:%s", stream.userID), content); err != nil {
					localLog.Error("failed to kill running streams")
				}
			}

			handler.HandleReader(reader)

			go func() {
				current := statsFn()
				for {
					select {
					case <-gCtx.Done():
						_ = reader.Close()
					case <-time.After(time.Second * 5):
						new := statsFn()
						avgBitate := (new.VideoDataInBytes - current.VideoDataInBytes + new.AudioDataInBytes - current.AudioDataInBytes) / 5 / 1024 * 8
						localLog.Debug("average bitrate: ", avgBitate)
						current = new
					case <-reader.Running():
						return
					}
				}
			}()
			go func() {
				tkn, err := structures.EncodeJwt(structures.JwtInternalRead{
					StreamID: stream.streamID,
				}, gCtx.Config().RTMP.Auth.JwtToken)
				if err != nil {
					localLog.Error("failed to create jwt token: ", err)
					return
				}

				ffprobeCtx, ffprobeCancel := context.WithTimeout(ctx, time.Second*5)
				probeData, err := ffprobe.Run(ffprobeCtx, fmt.Sprintf("rtmp://%s/internal/%s", gCtx.Config().Pod.IP, hex.EncodeToString(utils.S2B(tkn))))
				ffprobeCancel()
				if err != nil {
					localLog.Error("failed to do ffprobe: ", err.Error())
					_ = reader.Close()
					return
				}

				audio := probeData.GetAudio()
				if audio.CodecType == "" {
					localLog.Error("could not find audio track")
					_ = reader.Close()
					return
				}
				video := probeData.GetVideo()
				if video.CodecType == "" {
					localLog.Error("could not find video track")
					_ = reader.Close()
					return
				}
			}()

			go func() {
				i := 0
				resetTimer := make(chan bool)
				defer close(resetTimer)
				go func() {
					timer := time.NewTimer(time.Minute)
					for {
						select {
						case <-reader.Running():
							return
						case v := <-resetTimer:
							if !v {
								return
							}
						case <-timer.C:
							i = 0
						}
						timer.Stop()
						timer.Reset(time.Minute)
					}
				}()
				for {
					select {
					case <-reader.Running():
						return
					default:
					}

					tkn, err := structures.EncodeJwt(structures.JwtTranscodePayload{
						StreamID:        stream.streamID,
						UserID:          stream.userID,
						TranscodeStream: true,
						Revision:        stream.revision,
						IngestPodIP:     gCtx.Config().Pod.IP,
					}, gCtx.Config().RTMP.Auth.JwtToken)
					if err != nil {
						localLog.Error("failed to create jwt token: ", err)
						return
					}

					conn := core.NewConnClient()
					if err := conn.Start(fmt.Sprintf("rtmp://%s/transcode/%s", gCtx.Config().RTMP.Transcoder.URL, hex.EncodeToString(utils.S2B(tkn))), av.PUBLISH); err != nil {
						i++
						resetTimer <- true
						if i > 5 {
							_ = reader.Close()
							return
						}
						time.Sleep(time.Second * 2)
						continue
					}

					writer := rtmp.NewVirWriter(conn, logrus.StandardLogger(), reader.Info(), nil)
					handler.HandleWriter(writer)

					<-writer.Running()

					i++
					resetTimer <- true
					if i > 5 {
						_ = reader.Close()
						return
					}

					time.Sleep(time.Second)

				}
			}()
			<-reader.Running()
		},
		HandleViewer: func(info av.Info, writer av.WriteCloser) {
			handler.HandleWriter(writer)
			<-writer.Running()
		},
	})

	ln, err := net.Listen("tcp", gCtx.Config().RTMP.Bind)
	if err != nil {
		logrus.Fatal("failed to listen to rtmp: ", err)
	}

	go func() {
		if err := server.Serve(ln); err != nil {
			logrus.Fatal("failed to listen to rtmp: ", err)
		}
		close(done)
	}()

	go func() {
		<-gCtx.Done()
		_ = server.Shutdown()
	}()

	return done
}
