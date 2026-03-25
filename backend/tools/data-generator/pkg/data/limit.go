package data

import (
	"fmt"
	"time"
)

// ----------------------------------------------------------------------------------

type (
	Range struct {
		StartDate, EndDate time.Time
		HourDateTime       time.Time
		HoursCount         int
	}
	Limit struct {
		Range
		NS, Services, Pods int
		Calls              int // calls per pods per 5 minutes
	}
)

// ----------------------------------------------------------------------------------

func (r Range) String() string {
	return fmt.Sprintf("[%v | %v]", r.StartDate, r.EndDate)
}

func (r Range) HasHourTime() bool {
	invalidTime := time.Now().AddDate(-1, 0, 0)
	return r.HourDateTime.After(invalidTime) && r.HourDateTime.Before(time.Now().Add(time.Hour))
}

func (r Range) IsDatesValid() bool { // to generate Parquet files per days
	return r.Count() < 24*31
}

func (r Range) IsHoursValid() bool { // to generate recent data in Postgres per hour
	return r.HoursCount > 0 && r.HoursCount < 4
}

func (r Range) Count() (hours int) {
	for t := r.StartDate; t.Before(r.EndDate) || t.Equal(r.EndDate); t = t.Add(time.Hour) {
		hours++
	}
	return hours
}

func (r Range) Hours() (hours []time.Time) {
	for t := r.StartDate; t.Before(r.EndDate) || t.Equal(r.EndDate); t = t.Add(time.Hour) {
		hours = append(hours, t)
	}
	return hours
}

func (r Range) RecentMinutes() (minutes []time.Time) {
	start, end := r.recentRange()
	for t := start; t.Before(end); t = t.Add(5 * time.Minute) {
		minutes = append(minutes, t)
	}
	return minutes
}

func (r Range) RecentHours() (minutes []time.Time) {
	start, end := r.recentRange()
	for t := start; t.Before(end); t = t.Add(time.Hour) {
		minutes = append(minutes, t)
	}
	return minutes
}

func (r Range) recentRange() (time.Time, time.Time) {
	start := r.HourDateTime.Truncate(time.Hour)
	end := start.Add(time.Duration(r.HoursCount) * time.Hour)
	return start, end
}

// ----------------------------------------------------------------------------------

func (l Limit) Cloud() string {
	return fmt.Sprintf("%d ns, %d svcs, %d replicas", l.NS, l.Services, l.Pods)
}

func (l Limit) Fix() Limit {
	if l.Calls < 0 {
		l.Calls = -l.Calls
	}
	return l
}
