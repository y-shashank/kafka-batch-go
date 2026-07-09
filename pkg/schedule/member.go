package schedule

import (
	"strconv"
	"strings"
)

// Member encodes scheduled-job pointers as job_id:partition:offset.
type Member struct {
	JobID     string
	Partition int32
	Offset    int64
}

func BuildMember(jobID string, partition int32, offset int64) string {
	return jobID + ":" + strconv.Itoa(int(partition)) + ":" + strconv.FormatInt(offset, 10)
}

func BuildKey(partition int32, offset int64) string {
	return strconv.Itoa(int(partition)) + ":" + strconv.FormatInt(offset, 10)
}

func ParseMember(member string) (Member, bool) {
	last := strings.LastIndex(member, ":")
	if last <= 0 {
		return Member{}, false
	}
	offsetStr := member[last+1:]
	rest := member[:last]
	mid := strings.LastIndex(rest, ":")
	if mid < 0 {
		return Member{}, false
	}
	jobID := rest[:mid]
	partStr := rest[mid+1:]
	if jobID == "" || partStr == "" || offsetStr == "" {
		return Member{}, false
	}
	part, err := strconv.Atoi(partStr)
	if err != nil {
		return Member{}, false
	}
	off, err := strconv.ParseInt(offsetStr, 10, 64)
	if err != nil {
		return Member{}, false
	}
	return Member{JobID: jobID, Partition: int32(part), Offset: off}, true
}
