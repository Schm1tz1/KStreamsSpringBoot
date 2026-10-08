package com.github.schm1tz1;

import java.lang.management.ManagementFactory;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;
import javax.management.JMException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * Test helper to read and print metrics from JMX. Both sides register their MBeans in this JVM's
 * platform MBeanServer: the app's Kafka clients through the clients' JmxReporter, and the embedded
 * broker through its Yammer metrics.
 */
final class MetricsReport {
  private static final MBeanServer MBEANS = ManagementFactory.getPlatformMBeanServer();

  private MetricsReport() {}

  /**
   * Client metrics: for every attribute of the matching MBeans, prints the Prometheus series
   * (Micrometer naming: kafka_ + type without "-metrics" + _ + attribute) with its scraped value,
   * plus the JMX MBean/attribute it is read from and its live JMX value. The two values differ when
   * Micrometer has not yet picked up a newly created metric (60s refresh).
   */
  static String client(String scrape, String mbeanPattern, Predicate<String> attributes) {
    StringBuilder sb = new StringBuilder();
    for (ObjectName n : query(mbeanPattern)) {
      String id =
          n.getKeyProperty("client-id") != null
              ? n.getKeyProperty("client-id")
              : n.getKeyProperty("thread-id");
      String group = n.getKeyProperty("type").replaceFirst("-metrics$", "");
      for (MBeanAttributeInfo a : info(n)) {
        if (!attributes.test(a.getName())) {
          continue;
        }
        String series = ("kafka_" + group + "_" + a.getName()).replace('-', '_');
        String line = scrapedLine(scrape, series, id);
        sb.append("\n  ")
            .append(line == null ? series : line.substring(0, line.indexOf('{')))
            .append('{')
            .append(id.replaceFirst(".*?(StreamThread-.*)", "$1"))
            .append("} = ")
            .append(
                line == null
                    ? "<not in /actuator/prometheus yet>"
                    : line.substring(line.lastIndexOf(' ') + 1))
            .append("\n    JMX ")
            .append(n)
            .append(" / ")
            .append(a.getName())
            .append(" = ")
            .append(attribute(n, a.getName()));
      }
    }
    return sb.toString();
  }

  /** Broker metric: label and value, plus the JMX MBean/attribute it is read from. */
  static String broker(String label, ObjectName n, String attribute) {
    return "\n  " + label + " = " + attribute(n, attribute) + "\n    JMX " + n + " / " + attribute;
  }

  /** Sum of the "Count" attribute (Yammer meters/histograms) over all matching MBeans. */
  static long count(String mbeanPattern) {
    return query(mbeanPattern).stream().mapToLong(n -> (Long) attribute(n, "Count")).sum();
  }

  static Set<ObjectName> query(String mbeanPattern) {
    try {
      return new TreeSet<>(MBEANS.queryNames(new ObjectName(mbeanPattern), null));
    } catch (JMException e) {
      throw new IllegalStateException(e);
    }
  }

  static Object attribute(ObjectName name, String attribute) {
    try {
      return MBEANS.getAttribute(name, attribute);
    } catch (JMException e) {
      throw new IllegalStateException(e);
    }
  }

  private static MBeanAttributeInfo[] info(ObjectName name) {
    try {
      return MBEANS.getMBeanInfo(name).getAttributes();
    } catch (JMException e) {
      throw new IllegalStateException(e);
    }
  }

  /** The scraped line of the series for this client/thread id, or null if not (yet) exported. */
  private static String scrapedLine(String scrape, String series, String id) {
    return scrape
        .lines()
        // counters without a "-total" attribute suffix get Prometheus' "_total" appended
        .filter(
            l ->
                (l.startsWith(series + "{") || l.startsWith(series + "_total{"))
                    && l.contains("\"" + id + "\""))
        .findFirst()
        .orElse(null);
  }
}
