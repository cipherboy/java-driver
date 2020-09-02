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

import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters keyspaces during schema metadata queries.
 *
 * <p>Depending on the circumstances, we do it either on the server side with a WHERE IN clause that
 * will be appended to every query, or on the client side with a predicate that will be applied to
 * every fetched row.
 */
class KeyspaceFilter {

  private static final Logger LOG = LoggerFactory.getLogger(KeyspaceFilter.class);

  private static final Pattern EXACT_NAME_INCLUDE = Pattern.compile("\\w+");
  private static final Pattern EXACT_NAME_EXCLUDE = Pattern.compile("!(\\w+)");
  private static final Pattern REGEX_INCLUDE = Pattern.compile("/(.+)/");
  private static final Pattern REGEX_EXCLUDE = Pattern.compile("!/(.+)/");

  private final String logPrefix;
  private final String whereClause;
  private final List<Predicate<String>> inclusions = new ArrayList<>();
  private final List<Predicate<String>> exclusions = new ArrayList<>();

  KeyspaceFilter(@NonNull String logPrefix, @NonNull List<String> specs) {
    this.logPrefix = logPrefix;
    List<String> exactNames = new ArrayList<>();
    for (String spec : specs) {
      spec = spec.trim();
      Matcher matcher;
      if (EXACT_NAME_INCLUDE.matcher(spec).matches()) {
        final String name = spec;
        exactNames.add(name);
        inclusions.add(s -> s.equals(name));
      } else if ((matcher = EXACT_NAME_EXCLUDE.matcher(spec)).matches()) {
        final String name = matcher.group(1);
        exclusions.add(s -> s.equals(name));
      } else if ((matcher = REGEX_INCLUDE.matcher(spec)).matches()) {
        compile(matcher.group(1)).map(inclusions::add);
      } else if ((matcher = REGEX_EXCLUDE.matcher(spec)).matches()) {
        compile(matcher.group(1)).map(exclusions::add);
      } else {
        LOG.warn(
            "[{}] Error while parsing {}: invalid element '{}', skipping",
            logPrefix,
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath(),
            spec);
      }
    }

    if (!inclusions.isEmpty() && exactNames.size() == inclusions.size()) {
      // Special case: all inclusions use exact names, we can filter on the server
      inclusions.clear();
      whereClause = buildWhereClause(exactNames);
      if (!exclusions.isEmpty()) {
        // Proceed, but this is dumb
        LOG.warn(
            "[{}] {} only includes explicit keyspace names, but also defines exclusions. "
                + "This can probably be simplified.",
            logPrefix,
            DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath());
      }
    } else {
      whereClause = "";
    }
  }

  @VisibleForTesting
  KeyspaceFilter(String... specs) {
    this("test", Arrays.asList(specs));
  }

  /** The WHERE IN clause, or the empty string if there is no server-side filtering. */
  @NonNull
  String getWhereClause() {
    return whereClause;
  }

  /** The predicate that will be invoked for client-side filtering. */
  boolean includes(@NonNull String keyspace) {
    return (inclusions.isEmpty() || matchesAny(keyspace, inclusions))
        && !matchesAny(keyspace, exclusions);
  }

  private boolean matchesAny(String keyspace, List<Predicate<String>> rules) {
    for (Predicate<String> rule : rules) {
      if (rule.test(keyspace)) {
        return true;
      }
    }
    return false;
  }

  private Optional<Predicate<String>> compile(String regex) {
    try {
      return Optional.of(Pattern.compile(regex).asPredicate());
    } catch (PatternSyntaxException e) {
      LOG.warn(
          "[{}] Error while parsing {}: syntax error in regex /{}/ ({}), skipping",
          this.logPrefix,
          DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES.getPath(),
          regex,
          e.getMessage());
      return Optional.empty();
    }
  }

  private static String buildWhereClause(List<String> keyspaces) {
    StringBuilder builder = new StringBuilder(" WHERE keyspace_name IN (");
    boolean first = true;
    for (String keyspace : keyspaces) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      builder.append('\'').append(keyspace).append('\'');
    }
    return builder.append(')').toString();
  }
}
