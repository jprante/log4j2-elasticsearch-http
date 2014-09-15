/**
 *    Copyright 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.logging.log4j2;

import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.nosql.appender.NoSqlProvider;

@Plugin(name = "Elasticsearch", category = "Core", printObject = true)
public class ElasticsearchHttpProvider implements NoSqlProvider<ElasticsearchHttpConnection> {

    private final ElasticsearchHttpClient client;

    private final String description;

    private ElasticsearchHttpProvider(final ElasticsearchHttpClient client, final String description) {
        this.client = client;
        this.description = "elasticsearch-http{ " + description + " }";
    }

    @Override
    public ElasticsearchHttpConnection getConnection() {
        return new ElasticsearchHttpConnection(client);
    }

    @Override
    public String toString() {
        return description;
    }

    /**
     * Factory method for creating an Elasticsearch provider within the plugin manager.
     *
     * @param url     The URL of a host in an Elasticsearch cluster to which log event documents will be written.
     * @param index   The index that Elasticsearch shall use for indexing
     * @param type    The type of the index Elasticsearch shall use for indexing
     * @return a new Elasticsearch provider
     */
    @PluginFactory
    public static ElasticsearchHttpProvider createNoSqlProvider(
            @PluginAttribute("url") String url,
            @PluginAttribute("index") String index,
            @PluginAttribute("type") String type,
            @PluginAttribute("create") Boolean create,
            @PluginAttribute("maxActionsPerBulkRequest") Integer maxActionsPerBulkRequest,
            @PluginAttribute("logResponses") Boolean logResponses) {
        if (url == null || url.isEmpty()) {
            url = "http://localhost:9200/_bulk";
        }
        if (index == null || index.isEmpty()) {
            index = "log4j2";
        }
        if (type == null || type.isEmpty()) {
            type = "log4j2";
        }
        if (maxActionsPerBulkRequest == null || maxActionsPerBulkRequest == 0) {
            maxActionsPerBulkRequest = 1000;
        }
        if (logResponses == null) {
            logResponses = false;
        }
        String description = "url=" + url + ",index=" + index + ",type=" + type;
        ElasticsearchHttpClient elasticsearchClient = new ElasticsearchHttpClient(url, index, type, create,
                maxActionsPerBulkRequest, 5, logResponses);
        return new ElasticsearchHttpProvider(elasticsearchClient, description);
    }

}
