package ingest

import (
	"github.com/viderstv/common/streaming/av"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type Stream struct {
	info     av.Info
	userID   primitive.ObjectID
	streamID primitive.ObjectID
	shutdown bool
	revision int32
}
