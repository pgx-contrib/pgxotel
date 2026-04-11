package pgxotel_test

import (
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestPgxotel(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "pgxotel Suite")
}
