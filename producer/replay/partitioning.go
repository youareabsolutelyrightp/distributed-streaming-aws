package replay

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
)

const (
	PartitioningVersion        = "taxi-medallion-partition-v1"
	PartitionHashVectorVersion = "sha256-prefix64-be-v1"
)

func PartitionForMedallion(medallion string, partitionCount int) (int32, error) {
	if partitionCount <= 0 {
		return 0, fmt.Errorf("partition count must be positive")
	}
	digest := sha256.Sum256([]byte(medallion))
	prefix := binary.BigEndian.Uint64(digest[:8])
	return int32(prefix % uint64(partitionCount)), nil
}
