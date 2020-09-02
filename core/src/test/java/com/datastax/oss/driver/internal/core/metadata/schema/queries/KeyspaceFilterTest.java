/*
 * Copyright DataStax, Inc.
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
package com.datastax.oss.driver.internal.core.metadata.schema.queries;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class KeyspaceFilterTest {

  @Test
  public void should_not_filter_when_no_rules() {
    KeyspaceFilter filter = new KeyspaceFilter();
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("system")).isTrue();
    assertThat(filter.includes("ks1")).isTrue();
  }

  @Test
  public void should_filter_on_server_when_only_exact_inclusions_and_no_exclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }

  @Test
  public void should_filter_on_server_and_client_when_only_exact_inclusions_and_some_exclusions() {
    // This is weird but supported
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "!/.*2/");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isFalse();
  }

  @Test
  public void should_filter_on_client_when_only_exclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("!system");
    assertThat(filter.getWhereClause()).isEmpty();
    assertThat(filter.includes("system")).isFalse();
    assertThat(filter.includes("ks1")).isTrue();
  }

  @Test
  public void should_filter_on_client_when_regex_inclusions() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "/KS.*/", "!KS2", "!/.*2/");
    assertThat(filter.getWhereClause()).isEmpty();
    // Matches "ks1"
    assertThat(filter.includes("ks1")).isTrue();
    // Matches "ks2", but also "!/.*2/"
    assertThat(filter.includes("ks2")).isFalse();
    // Matches "/KS.*/"
    assertThat(filter.includes("KS1")).isTrue();
    // Matches /KS.*/, but also "!KS2"
    assertThat(filter.includes("KS2")).isFalse();
  }

  @Test
  public void should_skip_malformed_rule() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "//");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }

  @Test
  public void should_skip_invalid_regex() {
    KeyspaceFilter filter = new KeyspaceFilter("ks1", "ks2", "/*/");
    assertThat(filter.getWhereClause()).isEqualTo(" WHERE keyspace_name IN ('ks1','ks2')");
    assertThat(filter.includes("ks1")).isTrue();
    assertThat(filter.includes("ks2")).isTrue();
  }
}
