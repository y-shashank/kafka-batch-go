package daemon

import "fmt"

func healthMemberKey(group string, member, members int) string {
	if group == "" {
		return ""
	}
	if members < 2 {
		return group
	}
	return fmt.Sprintf("%s#m%d", group, member)
}

func memberLabel(member, members int) string {
	if members < 1 {
		members = 1
	}
	if member < 1 {
		member = 1
	}
	return fmt.Sprintf("%d/%d", member, members)
}
