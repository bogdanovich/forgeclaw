package evolution

import (
	"fmt"
	"time"
)

type LifecycleRunSummary struct {
	EvaluatedProfiles    int
	TransitionedProfiles int
	DeletedSkills        int
}

func NextLifecycleState(profile SkillProfile, now time.Time) SkillStatus {
	if profile.Origin == "manual" || profile.LastUsedAt.IsZero() {
		return profile.Status
	}

	idle := now.Sub(profile.LastUsedAt)
	switch profile.Status {
	case SkillStatusActive:
		if idle > 90*24*time.Hour && profile.RetentionScore < 0.3 {
			return SkillStatusCold
		}
	case SkillStatusCold:
		if idle > 180*24*time.Hour && profile.RetentionScore < 0.2 {
			return SkillStatusArchived
		}
	case SkillStatusArchived:
		if idle > 365*24*time.Hour && profile.RetentionScore < 0.1 {
			return SkillStatusArchived
		}
	}

	return profile.Status
}

func ApplyLifecycleState(paths Paths, profile SkillProfile, next SkillStatus) error {
	if next == SkillStatusDeleted {
		return fmt.Errorf("automatic skill deletion is disabled")
	}
	return nil
}

func RunLifecycleOnce(store *Store, paths Paths, workspace string, now time.Time) (LifecycleRunSummary, error) {
	if store == nil {
		return LifecycleRunSummary{}, nil
	}

	profiles, err := store.LoadProfiles()
	if err != nil {
		return LifecycleRunSummary{}, err
	}

	summary := LifecycleRunSummary{}
	for _, profile := range profiles {
		if !profileBelongsToWorkspace(paths, workspace, profile) {
			continue
		}

		summary.EvaluatedProfiles++
		next := NextLifecycleState(profile, now)
		if next == profile.Status {
			continue
		}

		if err := ApplyLifecycleState(paths, profile, next); err != nil {
			return summary, err
		}
		profile.VersionHistory = append(profile.VersionHistory, SkillVersionEntry{
			Version:   profile.CurrentVersion,
			Action:    "lifecycle:" + string(next),
			Timestamp: now,
			Summary:   fmt.Sprintf("lifecycle transition: %s -> %s", profile.Status, next),
		})
		profile.Status = next
		if err := store.SaveProfile(profile); err != nil {
			return summary, err
		}

		summary.TransitionedProfiles++
		if next == SkillStatusDeleted {
			summary.DeletedSkills++
		}
	}

	return summary, nil
}

func profileBelongsToWorkspace(paths Paths, workspace string, profile SkillProfile) bool {
	if profile.WorkspaceID == workspace {
		return true
	}
	return profile.WorkspaceID == "" && usesDefaultWorkspaceState(paths, workspace)
}

func usesDefaultWorkspaceState(paths Paths, workspace string) bool {
	return paths.RootDir == NewPaths(workspace, "").RootDir
}
