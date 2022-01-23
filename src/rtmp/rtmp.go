package rtmp

import (
	"context"
	"fmt"
	"net"
	"os/exec"
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
	"github.com/viderstv/ingest/src/global"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var json = jsoniter.ConfigCompatibleWithStandardLibrary

func New(gCtx global.Context) <-chan struct{} {
	done := make(chan struct{})

	handler := handler.New()

	mp := map[string]*Stream{}
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
			}
			mtx.Unlock()
		},
		AuthStream: func(info *av.Info, addr net.Addr) bool {
			start := time.Now()
			if !info.Publisher {
				if info.App == "internal" && utils.AddressIsInternal(addr) {
					info.Key = info.Name
					return true
				}
			}

			if info.Publisher && info.App != "live" {
				return false
			}

			if !strings.HasPrefix(info.Name, "live_") {
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
					logrus.Error("failed to query user: ", err)
				}
				return false
			}
			if user.Role < structures.GlobalRoleStreamer {
				return false
			}

			var sID primitive.ObjectID
			_sID, err := gCtx.Inst().Redis.Get(gCtx, fmt.Sprintf("unclean-shutdown:%s", uID.Hex()))
			if err != nil && err != redis.Nil {
				logrus.Error("failed to query redis: ", err)
			} else if err != redis.Nil {
				_ = gCtx.Inst().Redis.Del(gCtx, fmt.Sprintf("unclean-shutdown:%s", uID.Hex()))
				sID, err = primitive.ObjectIDFromHex(_sID.(string))
				if err != nil {
					logrus.Error("invalid entry in redis: ", err)
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
					logrus.Error("failed to fetch old stream: ", err)
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
					logrus.Error("failed to insert new stream: ", err)
					return false
				}
			}

			if _, err := gCtx.Inst().Mongo.Collection(mongo.CollectionNameUsers).UpdateOne(gCtx, bson.M{"_id": uID}, bson.M{
				"$set": bson.M{
					"channel.last_live": start,
				},
			}); err != nil {
				logrus.Error("failed to update last live: ", err)
			}

			mtx.Lock()
			mp[info.ID] = &Stream{
				info:     *info,
				userID:   uID,
				streamID: sID,
				revision: revision,
			}
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
				ffprobeCtx, ffprobeCancel := context.WithTimeout(ctx, time.Second*5)
				data, err := exec.CommandContext(ffprobeCtx, "ffprobe",
					"-v", "quiet",
					"-print_format", "json",
					"-show_format",
					"-show_streams",
					fmt.Sprintf("rtmp://127.0.0.1:1935/internal/%s", info.Key), // rtmp url
				).CombinedOutput()
				ffprobeCancel()
				if err != nil {
					localLog.Errorf("failed to read stream: %s %s", err.Error(), data)
					return
				}
				probeData := FFProbeData{}
				if err := json.Unmarshal(data, &probeData); err != nil {
					localLog.Error("failed to read stream: ", err)
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
				for {
					select {
					case <-reader.Running():
						return
					default:
					}

					conn := core.NewConnClient()
					if err := conn.Start(fmt.Sprintf("%s/streamid/%s", gCtx.Config().RTMP.PushURL, stream.streamID.Hex()), av.PUBLISH); err != nil {
						i++
						if i > 5 {
							_ = reader.Close()
							return
						}
						time.Sleep(time.Second * 2)
						continue
					}

					i = 0
					writer := rtmp.NewVirWriter(conn, logrus.StandardLogger(), reader.Info(), nil)
					handler.HandleWriter(writer)

					<-writer.Running()
				}
			}()
			handler.HandleReader(reader)
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
