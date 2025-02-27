/*
Copyright 2025 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package usageclients

// NotSupportedError is an error to be used by all usage clients when they do
// not support some kind of operation. Either it may be not yet implemented or
// it is not supported by the usage client at all.
type NotSupportedError struct {
	usageClientType UsageClientType
	message         string
}

// Error returns the error message.
func (e NotSupportedError) Error() string {
	return e.message
}

// ClientType returns the usage client type that raised the error.
func (e NotSupportedError) ClientType() UsageClientType {
	return e.usageClientType
}

// NewNotSupportedError creates a new NotSupportedError.
func NewNotSupportedError(ctype UsageClientType, msg string) *NotSupportedError {
	return &NotSupportedError{
		usageClientType: ctype,
		message:         msg,
	}
}
