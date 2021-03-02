package balancer

import (
	"math/rand"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Balancer", func() {
	var subject *Balancer

	It("should initialize with defaults", func() {
		subject = New([]Options{}, ModeFirstUp)
		defer subject.Close()

		Expect(subject.selector).To(HaveLen(1))
		Expect(subject.selector[0].Option().Addr).To(Equal("127.0.0.1:4150"))
	})

	Describe("Next", func() {

		BeforeEach(func() {
			rand.Seed(100)
			ms := int64(time.Millisecond)
			subject.selector = pool{
				&nsqBackend{opt: mockOpts("host-1:4150"), up: 0, connections: 0, latency: ms},
				&nsqBackend{opt: mockOpts("host-2:4150"), up: 1, connections: 10, latency: 2 * ms},
				&nsqBackend{opt: mockOpts("host-3:4150"), up: 1, connections: 8, latency: 3 * ms},
				&nsqBackend{opt: mockOpts("host-4:4150"), up: 1, connections: 14, latency: ms},
			}
		})

		It("should pick next backend (first-up)", func() {
			subject.mode = ModeFirstUp
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.selector[1].Connections()).To(Equal(int64(14)))
		})

		It("should pick next backend (least-conn)", func() {
			subject.mode = ModeLeastConn
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.selector[1].Connections()).To(Equal(int64(12)))
			Expect(subject.selector[2].Connections()).To(Equal(int64(11)))
		})

		It("should pick next backend (min-latency)", func() {
			subject.mode = ModeMinLatency
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.selector[3].Connections()).To(Equal(int64(18)))
		})

		It("should pick next backend (randomly)", func() {
			subject.mode = ModeRandom
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.selector[3].Connections()).To(Equal(int64(15)))
		})

		It("should pick next backend (weighted-latency)", func() {
			subject.mode = ModeWeightedLatency
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.selector[1].Connections()).To(Equal(int64(12)))
			Expect(subject.selector[3].Connections()).To(Equal(int64(17)))
		})

		It("should pick next backend (round-robin)", func() {
			subject.mode = ModeRoundRobin
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-2:4150"))
			Expect(subject.selector[3].Connections()).To(Equal(int64(16)))
		})

		It("should fallback on random when everything down", func() {
			for i := 1; i <= 3; i++ {
				b := subject.selector[i].(*nsqBackend)
				b.up = 0
			}

			Expect(subject.pickNext().Option().Addr).To(Equal("host-4:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-1:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-1:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-1:4150"))
			Expect(subject.pickNext().Option().Addr).To(Equal("host-3:4150"))
		})

	})

})

// --------------------------------------------------------------------

func TestSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "nsq-balancer")
}

func mockOpts(addr string) *Options {
	return &Options{
		Addr: addr,
	}
}
