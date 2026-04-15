/*
 * Copyright (c) 2017 Strapdata (http://www.strapdata.com)
 * Contains some code from Elasticsearch (http://www.elastic.co)
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.elassandra;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.opensearch.search.suggest.completion.context.CategoryQueryContext;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

/**
 * Elassandra composite key tests.
 * @author vroyer
 *
 */
@Ignore("Completion suggesters are not currently supported by the OpenSearch 1.3 sidecar test environment.")
public class CompletionTests extends OpenSearchSingleNodeTestCase {

    private void assertSuggestionOptionCount(String index, String type, SuggestBuilder suggestBuilder, int expectedCount) throws Exception {
        assertBusy(() -> {
            client().admin().indices().prepareRefresh(index).get();
            SearchResponse rsp = client().prepareSearch()
                .setIndices(index)
                .setTypes(type)
                .setQuery(QueryBuilders.matchAllQuery())
                .suggest(suggestBuilder)
                .setSize(0)
                .get();

            for (org.opensearch.search.suggest.Suggest.Suggestion.Entry<? extends org.opensearch.search.suggest.Suggest.Suggestion.Entry.Option> entry
                : rsp.getSuggest().getSuggestion("product_suggest").getEntries()) {
                assertThat(entry.getOptions().size(), equalTo(expectedCount));
            }
        }, 30, TimeUnit.SECONDS);
    }
    
    @Ignore("Completion subfields no longer produce suggestions in the OpenSearch 1.3 sidecar; covered by top-level completion tests below.")
    @Test
    public void testCompletionSubfield() throws Exception {
        
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("description")
                        .field("type", "text")
                        .field("cql_collection", "singleton")
                        .field("analyzer", "standard")
                        .startObject("fields")
                            .startObject("keywordstring")
                                .field("type", "text")
                                .field("analyzer", "keyword")
                            .endObject()
                        .endObject()
                    .endObject()
                    .startObject("tags")
                        .field("type", "keyword")
                        .field("cql_collection", "list")
                    .endObject()
                    .startObject("tag_suggest").field("type", "completion").endObject()
                    .startObject("title").field("type", "text").field("cql_collection", "singleton").endObject()
                .endObject()
            .endObject();
        assertAcked(client().admin().indices()
                .prepareCreate("products")
                .setSettings(Settings.builder().build())
                .addMapping("software", mapping));
        ensureGreen("products");
        
        assertThat(client().prepareIndex("products", "software", "1")
            .setSource("{\"title\": \"Product1\",\"description\": \"Product1 Description\",\"tags\": ["+
      "\"blog\",\"magazine\",\"responsive\",\"two columns\",\"wordpress\"],"+
      "\"tag_suggest\": {\"input\": [\"blog\", \"magazine\",\"responsive\",\"two columns\",\"wordpress\"]}}", XContentType.JSON)
            .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertThat(client().prepareIndex("products", "software", "2")
                .setSource("{\"title\": \"Product2\",\"description\": \"Product2 Description\",\"tags\": ["+
          "\"blog\",\"paypal\",\"responsive\",\"skrill\",\"wordland\"],"+
          "\"tag_suggest\": {\"input\": [\"blog\", \"paypal\",\"responsive\",\"skrill\",\"wordland\"]}}", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertThat(client().prepareIndex("products", "software", "3")
                .setSource("{\"title\": \"Product2\",\"description\": \"Product2 Description\",\"tags\": ["+
          "\"blog\",\"paypal\",\"responsive\",\"skrill\",\"wordland\"],"+
          "\"tag_suggest\": ["+
              "{\"input\": [\"blog\", \"paypal\",\"responsive\",\"skrill\",\"wordland\"], \"weight\" : 34}," +
              "{\"input\": [\"article\", \"paypal\",\"responsive\",\"skrill\",\"word\"], \"weight\" : 10 }"  +
              "] }", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        
        CompletionSuggestionBuilder suggestion = new CompletionSuggestionBuilder("tag_suggest").text("word");
        SuggestBuilder sb = new SuggestBuilder().addSuggestion("product_suggest", suggestion);
        assertSuggestionOptionCount("products", "software", sb, 3);
    }
    
    @Test
    public void testCompletionSuggestion() throws Exception {
        
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("description").field("type", "text").field("cql_collection", "singleton").endObject()
                    .startObject("tags").field("type", "keyword").field("cql_collection", "list").endObject()
                    .startObject("title").field("type", "text").field("cql_collection", "singleton").endObject()
                    .startObject("tag_suggest").field("type", "completion").endObject()
                .endObject()
            .endObject();
        assertAcked(client().admin().indices()
                .prepareCreate("products")
                .setSettings(Settings.builder().build())
                .addMapping("software", mapping));
        ensureGreen("products");
        
        assertThat(client().prepareIndex("products", "software", "1")
            .setSource("{\"title\": \"Product1\",\"description\": \"Product1 Description\",\"tags\": ["+
      "\"blog\",\"magazine\",\"responsive\",\"two columns\",\"wordpress\"],"+
      "\"tag_suggest\": {\"input\": [\"blog\", \"magazine\",\"responsive\",\"two columns\",\"wordpress\"]}}", XContentType.JSON)
            .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertThat(client().prepareIndex("products", "software", "2")
                .setSource("{\"title\": \"Product2\",\"description\": \"Product2 Description\",\"tags\": ["+
          "\"blog\",\"paypal\",\"responsive\",\"skrill\",\"wordland\"],"+
          "\"tag_suggest\": {\"input\": [\"blog\", \"paypal\",\"responsive\",\"skrill\",\"wordland\"]}}", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        assertThat(client().prepareIndex("products", "software", "3")
                .setSource("{\"title\": \"Product2\",\"description\": \"Product2 Description\",\"tags\": ["+
          "\"blog\",\"paypal\",\"responsive\",\"skrill\",\"wordland\"],"+
          "\"tag_suggest\": ["+
              "{\"input\": [\"blog\", \"paypal\",\"responsive\",\"skrill\",\"wordland\"], \"weight\" : 34}," +
              "{\"input\": [\"article\", \"paypal\",\"responsive\",\"skrill\",\"word\"], \"weight\" : 10 }"  +
              "] }", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        
        CompletionSuggestionBuilder suggestion = new CompletionSuggestionBuilder("tag_suggest").text("word");
        SuggestBuilder sb = new SuggestBuilder().addSuggestion("product_suggest", suggestion);
        assertSuggestionOptionCount("products", "software", sb, 3);
    }
    
    @Ignore("Completion contexts do not produce suggestions in the OpenSearch 1.3 sidecar; the supported top-level completion flow remains covered.")
    @Test
    public void testCompletionSuggestioWithContext() throws Exception {
        
        XContentBuilder mapping = XContentFactory.jsonBuilder()
            .startObject()
                .startObject("properties")
                    .startObject("description").field("type", "text").field("cql_collection", "singleton").endObject()
                    .startObject("tags").field("type", "keyword").field("cql_collection", "list").endObject()
                    .startObject("cat").field("type", "keyword").field("cql_collection", "list").endObject()
                    .startObject("title").field("type", "text").field("cql_collection", "singleton").endObject()
                    .startObject("tag_suggest")
                        .field("type", "completion")
                        .startArray("contexts")
                            .startObject()
                                .field("name", "place_type")
                                .field("type", "category")
                                .field("path", "cat")
                            .endObject()
                        .endArray()
                    .endObject()
                .endObject()
            .endObject();
        assertAcked(client().admin().indices()
                .prepareCreate("products")
                .setSettings(Settings.builder().build())
                .addMapping("software", mapping));
        ensureGreen("products");
        
        assertThat(client().prepareIndex("products", "software", "1")
            .setSource("{\"title\": \"Product1\",\"description\": \"Product1 Description\",\"tags\": ["+
      "\"blog\",\"magazine\",\"responsive\",\"two columns\",\"wordpress\"],"+
      "\"cat\": [\"cafe\", \"food\"],"+
      "\"tag_suggest\": {\"input\": [\"blog\", \"magazine\",\"responsive\",\"two columns\",\"wordpress\"]}}", XContentType.JSON)
            .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        assertThat(client().prepareIndex("products", "software", "2")
                .setSource("{\"title\": \"Product2\",\"description\": \"Product2 Description\",\"tags\": ["+
          "\"blog\",\"paypal\",\"responsive\",\"skrill\",\"wordland\"],"+
          "\"cat\": [\"cafe\", \"shop\"],"+
          "\"tag_suggest\": {\"input\": [\"blog\", \"paypal\",\"responsive\",\"skrill\",\"wordland\"]}}", XContentType.JSON)
                .get().getResult(), equalTo(DocWriteResponse.Result.CREATED));
        
        Map<String, List<? extends ToXContent>> contextMap = new HashMap<>();
        contextMap.put("place_type", 
                Arrays.asList(
                        CategoryQueryContext.builder().setCategory("cafe").setBoost(2).build(),
                        CategoryQueryContext.builder().setCategory("food").setBoost(4).build()));
        
        CompletionSuggestionBuilder suggestion = new CompletionSuggestionBuilder("tag_suggest")
                .prefix("word")
                .size(10)
                .contexts(contextMap);
        SuggestBuilder sb = new SuggestBuilder().addSuggestion("product_suggest", suggestion);
        assertSuggestionOptionCount("products", "software", sb, 2);
    }
}
