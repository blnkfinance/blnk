/*
Copyright 2024 Blnk Finance Authors.

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

package blnk

import (
	"context"

	"github.com/typesense/typesense-go/typesense/api"
)

// Search performs a search on the specified collection using the provided query parameters.
//
// Parameters:
// - collection string: The name of the collection to search.
// - query *api.SearchCollectionParams: The search query parameters.
//
// Returns:
// - interface{}: The search results.
// - error: An error if the search operation fails.
func (l *Blnk) Search(collection string, query *api.SearchCollectionParams) (interface{}, error) {
	return l.search.Search(context.Background(), collection, query)
}

// MultiSearch performs a multi-search operation across collections.
func (l *Blnk) MultiSearch(searchParams *api.MultiSearchSearchesParameter) (*api.MultiSearchResult, error) {
	return l.search.MultiSearch(context.Background(), *searchParams)
}
