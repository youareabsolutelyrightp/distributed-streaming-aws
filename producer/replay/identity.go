package replay

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
)

func ExpectedEventID(datasetID, joinedRecordID string) string {
	digest := sha256.Sum256([]byte(datasetID + joinedRecordID))
	return hex.EncodeToString(digest[:])
}

func ValidateEventIdentity(datasetID, joinedRecordID, eventID string) error {
	if datasetID == "" || joinedRecordID == "" {
		return errors.New("event identity is missing dataset_id or joined_record_id")
	}
	if eventID != ExpectedEventID(datasetID, joinedRecordID) {
		return errors.New("event_id does not match dataset_id and joined_record_id")
	}
	return nil
}
