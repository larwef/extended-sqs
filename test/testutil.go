package test

import (
	"reflect"
	"testing"
)

// AssertNotError asserts if an error equals nil or fails the test
func AssertNotError(t *testing.T, err error) {
	if err != nil {
		t.Fatalf("Got unexpected error: %s", err)
	}
}

// AssertIsError asserts if an error equals nil and and fails the test if its not
func AssertIsError(t *testing.T, err error) {
	if err == nil {
		t.Fatal("Expected error. Was nil.")
	}
}

// AssertEqual asserts if two object are same type and equal value
func AssertEqual(t *testing.T, actual interface{}, expected interface{}) {
	if actual != expected {
		t.Errorf("Expected %v %v to be equal to %v %v", reflect.TypeOf(actual).Name(), actual, reflect.TypeOf(expected).Name(), expected)
	}
}
