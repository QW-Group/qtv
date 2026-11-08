package qtv

import (
	"strconv"

	"github.com/rs/zerolog/log"
)

// onChangeQueueSize accepts positive decimal byte budgets portable to 32-bit hosts.
// Existing queues retain the limit captured when their stream was created.
func onChangeQueueSize(qtv *QTV, name string, cur, next *qVar) bool {
	n, err := strconv.ParseInt(next.Str, 10, 32)
	if err != nil || n <= 0 {
		log.Error().Str("ctx", "qVarStorage").Msgf("%s must be a positive integer byte size, at most 2147483647", name)
		return false
	}
	return true
}
