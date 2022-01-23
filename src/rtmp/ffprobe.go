package rtmp

type FFProbeData struct {
	Streams []FFProbeDataStream `json:"streams"`
	Format  FFProbeDataFormat   `json:"format"`
}

func (d FFProbeData) GetAudio() FFProbeDataStream {
	for _, v := range d.Streams {
		if v.CodecType == "audio" {
			return v
		}
	}

	return FFProbeDataStream{}
}

func (d FFProbeData) GetVideo() FFProbeDataStream {
	for _, v := range d.Streams {
		if v.CodecType == "video" {
			return v
		}
	}

	return FFProbeDataStream{}
}

type FFProbeDataStream struct {
	AvgFrameRate       string                       `json:"avg_frame_rate"`
	CodecName          string                       `json:"codec_name"`
	CodecType          string                       `json:"codec_type"`
	SampleFmt          string                       `json:"sample_fmt"`
	BitsPerSample      float64                      `json:"bits_per_sample"`
	RFrameRate         string                       `json:"r_frame_rate"`
	Profile            string                       `json:"profile"`
	Channels           float64                      `json:"channels"`
	StartPts           float64                      `json:"start_pts"`
	BitRate            string                       `json:"bit_rate"`
	Index              float64                      `json:"index"`
	ChannelLayout      string                       `json:"channel_layout"`
	TimeBase           string                       `json:"time_base"`
	CodecLongName      string                       `json:"codec_long_name"`
	CodecTagString     string                       `json:"codec_tag_string"`
	CodecTag           string                       `json:"codec_tag"`
	SampleRate         string                       `json:"sample_rate"`
	StartTime          string                       `json:"start_time"`
	FilmGrain          float64                      `json:"film_grain"`
	HasBFrames         float64                      `json:"has_b_frames"`
	ColorSpace         string                       `json:"color_space"`
	FieldOrder         string                       `json:"field_order"`
	ClosedCaptions     float64                      `json:"closed_captions"`
	PixFmt             string                       `json:"pix_fmt"`
	Level              float64                      `json:"level"`
	ColorTransfer      string                       `json:"color_transfer"`
	IsAvc              string                       `json:"is_avc"`
	SampleAspectRatio  string                       `json:"sample_aspect_ratio"`
	ColorRange         string                       `json:"color_range"`
	ColorPrimaries     string                       `json:"color_primaries"`
	NalLengthSize      string                       `json:"nal_length_size"`
	Height             float64                      `json:"height"`
	DisplayAspectRatio string                       `json:"display_aspect_ratio"`
	CodedHeight        float64                      `json:"coded_height"`
	Width              float64                      `json:"width"`
	CodedWidth         float64                      `json:"coded_width"`
	ChromaLocation     string                       `json:"chroma_location"`
	Refs               float64                      `json:"refs"`
	BitsPerRawSample   string                       `json:"bits_per_raw_sample"`
	Disposition        FFProbeDataStreamDisposition `json:"disposition"`
}

type FFProbeDataStreamDisposition struct {
	AttachedPic     float64 `json:"attached_pic"`
	TimedThumbnails float64 `json:"timed_thumbnails"`
	Metadata        float64 `json:"metadata"`
	HearingImpaired float64 `json:"hearing_impaired"`
	Karaoke         float64 `json:"karaoke"`
	CleanEffects    float64 `json:"clean_effects"`
	Descriptions    float64 `json:"descriptions"`
	Comment         float64 `json:"comment"`
	Original        float64 `json:"original"`
	Lyrics          float64 `json:"lyrics"`
	Dependent       float64 `json:"dependent"`
	Dub             float64 `json:"dub"`
	Forced          float64 `json:"forced"`
	VisualImpaired  float64 `json:"visual_impaired"`
	Captions        float64 `json:"captions"`
	StillImage      float64 `json:"still_image"`
	Default         float64 `json:"default"`
}

type FFProbeDataFormat struct {
	FormatLongName string            `json:"format_long_name"`
	Duration       string            `json:"duration"`
	Filename       string            `json:"filename"`
	NbPrograms     float64           `json:"nb_programs"`
	FormatName     string            `json:"format_name"`
	StartTime      string            `json:"start_time"`
	ProbeScore     float64           `json:"probe_score"`
	Tags           map[string]string `json:"tags"`
	NbStreams      float64           `json:"nb_streams"`
}
