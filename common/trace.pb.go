

package common

import (
	fmt "fmt"
	proto "github.com/gogo/protobuf/proto"
	io "io"
	math "math"
	math_bits "math/bits"
	"sync"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion3 // please upgrade the proto package

// protoc --gofast_out=. trace.proto
// protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=. trace.proto
// protoc -I=. -I=$GOPATH/src -I=$GOPATH/src/github.com/gogo/protobuf/protobuf --gogofaster_out=. trace.proto
type TraceData struct {
	Id     string      `protobuf:"bytes,1,opt,name=Id,proto3" json:"Id,omitempty"`
	Source string      `protobuf:"bytes,2,opt,name=Source,proto3" json:"Source,omitempty"`
	Md5    string      `protobuf:"bytes,3,opt,name=Md5,proto3" json:"Md5,omitempty"`
	Wrong  bool        `protobuf:"varint,4,opt,name=Wrong,proto3" json:"Wrong,omitempty"`
	Sd     []*SpanData `protobuf:"bytes,5,rep,name=Sd,proto3" json:"Sd,omitempty"`
	sync.Mutex
}

func (m *TraceData) Reset()         { *m = TraceData{} }
func (m *TraceData) String() string { return proto.CompactTextString(m) }
func (*TraceData) ProtoMessage()    {}
func (*TraceData) Descriptor() ([]byte, []int) {
	return fileDescriptor_0571941a1d628a80, []int{0}
}
func (m *TraceData) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TraceData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TraceData.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *TraceData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TraceData.Merge(m, src)
}
func (m *TraceData) XXX_Size() int {
	return m.Size()
}
func (m *TraceData) XXX_DiscardUnknown() {
	xxx_messageInfo_TraceData.DiscardUnknown(m)
}

var xxx_messageInfo_TraceData proto.InternalMessageInfo

func (m *TraceData) GetId() string {
	if m != nil {
		return m.Id
	}
	return ""
}

func (m *TraceData) GetSource() string {
	if m != nil {
		return m.Source
	}
	return ""
}

func (m *TraceData) GetMd5() string {
	if m != nil {
		return m.Md5
	}
	return ""
}

func (m *TraceData) GetWrong() bool {
	if m != nil {
		return m.Wrong
	}
	return false
}

func (m *TraceData) GetSd() []*SpanData {
	if m != nil {
		return m.Sd
	}
	return nil
}

type SpanData struct {
	TraceId   string `protobuf:"bytes,1,opt,name=TraceId,proto3" json:"TraceId,omitempty"`
	StartTime string `protobuf:"bytes,2,opt,name=StartTime,proto3" json:"StartTime,omitempty"`
	Tags      []byte `protobuf:"bytes,3,opt,name=Tags,proto3" json:"Tags,omitempty"`
	Wrong     bool   `protobuf:"varint,4,opt,name=Wrong,proto3" json:"Wrong,omitempty"`
}

func (m *SpanData) Reset()         { *m = SpanData{} }
func (m *SpanData) String() string { return proto.CompactTextString(m) }
func (*SpanData) ProtoMessage()    {}
func (*SpanData) Descriptor() ([]byte, []int) {
	return fileDescriptor_0571941a1d628a80, []int{1}
}
func (m *SpanData) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *SpanData) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_SpanData.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalToSizedBuffer(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (m *SpanData) XXX_Merge(src proto.Message) {
	xxx_messageInfo_SpanData.Merge(m, src)
}
func (m *SpanData) XXX_Size() int {
	return m.Size()
}
func (m *SpanData) XXX_DiscardUnknown() {
	xxx_messageInfo_SpanData.DiscardUnknown(m)
}

var xxx_messageInfo_SpanData proto.InternalMessageInfo

func (m *SpanData) GetTraceId() string {
	if m != nil {
		return m.TraceId
	}
	return ""
}

func (m *SpanData) GetStartTime() string {
	if m != nil {
		return m.StartTime
	}
	return ""
}

func (m *SpanData) GetTags() []byte {
	if m != nil {
		return m.Tags
	}
	return nil
}

func (m *SpanData) GetWrong() bool {
	if m != nil {
		return m.Wrong
	}
	return false
}

func init() {
	proto.RegisterType((*TraceData)(nil), "common.TraceData")
	proto.RegisterType((*SpanData)(nil), "common.SpanData")
}

func init() { proto.RegisterFile("trace.proto", fileDescriptor_0571941a1d628a80) }

var fileDescriptor_0571941a1d628a80 = []byte{
	// 228 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0xe2, 0xe2, 0x2e, 0x29, 0x4a, 0x4c,
	0x4e, 0xd5, 0x2b, 0x28, 0xca, 0x2f, 0xc9, 0x17, 0x62, 0x4b, 0xce, 0xcf, 0xcd, 0xcd, 0xcf, 0x53,
	0xaa, 0xe5, 0xe2, 0x0c, 0x01, 0x09, 0xbb, 0x24, 0x96, 0x24, 0x0a, 0xf1, 0x71, 0x31, 0x79, 0xa6,
	0x48, 0x30, 0x2a, 0x30, 0x6a, 0x70, 0x06, 0x31, 0x79, 0xa6, 0x08, 0x89, 0x71, 0xb1, 0x05, 0xe7,
	0x97, 0x16, 0x25, 0xa7, 0x4a, 0x30, 0x81, 0xc5, 0xa0, 0x3c, 0x21, 0x01, 0x2e, 0x66, 0xdf, 0x14,
	0x53, 0x09, 0x66, 0xb0, 0x20, 0x88, 0x29, 0x24, 0xc2, 0xc5, 0x1a, 0x5e, 0x94, 0x9f, 0x97, 0x2e,
	0xc1, 0xa2, 0xc0, 0xa8, 0xc1, 0x11, 0x04, 0xe1, 0x08, 0x29, 0x70, 0x31, 0x05, 0xa7, 0x48, 0xb0,
	0x2a, 0x30, 0x6b, 0x70, 0x1b, 0x09, 0xe8, 0x41, 0x6c, 0xd4, 0x0b, 0x2e, 0x48, 0xcc, 0x03, 0xd9,
	0x16, 0xc4, 0x14, 0x9c, 0xa2, 0x94, 0xc3, 0xc5, 0x01, 0xe3, 0x0b, 0x49, 0x70, 0xb1, 0x83, 0x9d,
	0x02, 0x77, 0x02, 0x8c, 0x2b, 0x24, 0xc3, 0xc5, 0x19, 0x5c, 0x92, 0x58, 0x54, 0x12, 0x92, 0x99,
	0x0b, 0x73, 0x0a, 0x42, 0x40, 0x48, 0x88, 0x8b, 0x25, 0x24, 0x31, 0xbd, 0x18, 0xec, 0x1c, 0x9e,
	0x20, 0x30, 0x1b, 0xbb, 0x7b, 0x9c, 0x24, 0x4e, 0x3c, 0x92, 0x63, 0xbc, 0xf0, 0x48, 0x8e, 0xf1,
	0xc1, 0x23, 0x39, 0xc6, 0x09, 0x8f, 0xe5, 0x18, 0x2e, 0x3c, 0x96, 0x63, 0xb8, 0xf1, 0x58, 0x8e,
	0x21, 0x89, 0x0d, 0x1c, 0x2a, 0xc6, 0x80, 0x00, 0x00, 0x00, 0xff, 0xff, 0x73, 0xb3, 0xa2, 0xe8,
	0x24, 0x01, 0x00, 0x00,
}

func (m *TraceData) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TraceData) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *TraceData) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if len(m.Sd) > 0 {
		for iNdEx := len(m.Sd) - 1; iNdEx >= 0; iNdEx-- {
			{
				size, err := m.Sd[iNdEx].MarshalToSizedBuffer(dAtA[:i])
				if err != nil {
					return 0, err
				}
				i -= size
				i = encodeVarintTrace(dAtA, i, uint64(size))
			}
			i--
			dAtA[i] = 0x2a
		}
	}
	if m.Wrong {
		i--
		if m.Wrong {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x20
	}
	if len(m.Md5) > 0 {
		i -= len(m.Md5)
		copy(dAtA[i:], m.Md5)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.Md5)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.Source) > 0 {
		i -= len(m.Source)
		copy(dAtA[i:], m.Source)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.Source)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.Id) > 0 {
		i -= len(m.Id)
		copy(dAtA[i:], m.Id)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.Id)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func (m *SpanData) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalToSizedBuffer(dAtA[:size])
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *SpanData) MarshalTo(dAtA []byte) (int, error) {
	size := m.Size()
	return m.MarshalToSizedBuffer(dAtA[:size])
}

func (m *SpanData) MarshalToSizedBuffer(dAtA []byte) (int, error) {
	i := len(dAtA)
	_ = i
	var l int
	_ = l
	if m.Wrong {
		i--
		if m.Wrong {
			dAtA[i] = 1
		} else {
			dAtA[i] = 0
		}
		i--
		dAtA[i] = 0x20
	}
	if len(m.Tags) > 0 {
		i -= len(m.Tags)
		copy(dAtA[i:], m.Tags)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.Tags)))
		i--
		dAtA[i] = 0x1a
	}
	if len(m.StartTime) > 0 {
		i -= len(m.StartTime)
		copy(dAtA[i:], m.StartTime)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.StartTime)))
		i--
		dAtA[i] = 0x12
	}
	if len(m.TraceId) > 0 {
		i -= len(m.TraceId)
		copy(dAtA[i:], m.TraceId)
		i = encodeVarintTrace(dAtA, i, uint64(len(m.TraceId)))
		i--
		dAtA[i] = 0xa
	}
	return len(dAtA) - i, nil
}

func encodeVarintTrace(dAtA []byte, offset int, v uint64) int {
	offset -= sovTrace(v)
	base := offset
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return base
}
func (m *TraceData) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.Id)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	l = len(m.Source)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	l = len(m.Md5)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	if m.Wrong {
		n += 2
	}
	if len(m.Sd) > 0 {
		for _, e := range m.Sd {
			l = e.Size()
			n += 1 + l + sovTrace(uint64(l))
		}
	}
	return n
}

func (m *SpanData) Size() (n int) {
	if m == nil {
		return 0
	}
	var l int
	_ = l
	l = len(m.TraceId)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	l = len(m.StartTime)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	l = len(m.Tags)
	if l > 0 {
		n += 1 + l + sovTrace(uint64(l))
	}
	if m.Wrong {
		n += 2
	}
	return n
}

func sovTrace(x uint64) (n int) {
	return (math_bits.Len64(x|1) + 6) / 7
}
func sozTrace(x uint64) (n int) {
	return sovTrace(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *TraceData) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTrace
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: TraceData: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TraceData: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Id", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Id = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Source", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Source = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Md5", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Md5 = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Wrong", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Wrong = bool(v != 0)
		case 5:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Sd", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				msglen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if msglen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + msglen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Sd = append(m.Sd, &SpanData{})
			if err := m.Sd[len(m.Sd)-1].Unmarshal(dAtA[iNdEx:postIndex]); err != nil {
				return err
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipTrace(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTrace
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTrace
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *SpanData) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTrace
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= uint64(b&0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: SpanData: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: SpanData: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TraceId", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.TraceId = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StartTime", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				stringLen |= uint64(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			intStringLen := int(stringLen)
			if intStringLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + intStringLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.StartTime = string(dAtA[iNdEx:postIndex])
			iNdEx = postIndex
		case 3:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Tags", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthTrace
			}
			postIndex := iNdEx + byteLen
			if postIndex < 0 {
				return ErrInvalidLengthTrace
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Tags = append(m.Tags[:0], dAtA[iNdEx:postIndex]...)
			if m.Tags == nil {
				m.Tags = []byte{}
			}
			iNdEx = postIndex
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Wrong", wireType)
			}
			var v int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				v |= int(b&0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			m.Wrong = bool(v != 0)
		default:
			iNdEx = preIndex
			skippy, err := skipTrace(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTrace
			}
			if (iNdEx + skippy) < 0 {
				return ErrInvalidLengthTrace
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipTrace(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	depth := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTrace
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
		case 1:
			iNdEx += 8
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowTrace
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if length < 0 {
				return 0, ErrInvalidLengthTrace
			}
			iNdEx += length
		case 3:
			depth++
		case 4:
			if depth == 0 {
				return 0, ErrUnexpectedEndOfGroupTrace
			}
			depth--
		case 5:
			iNdEx += 4
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
		if iNdEx < 0 {
			return 0, ErrInvalidLengthTrace
		}
		if depth == 0 {
			return iNdEx, nil
		}
	}
	return 0, io.ErrUnexpectedEOF
}

var (
	ErrInvalidLengthTrace        = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTrace          = fmt.Errorf("proto: integer overflow")
	ErrUnexpectedEndOfGroupTrace = fmt.Errorf("proto: unexpected end of group")
)
