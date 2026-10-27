import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.nessie.NessieCatalog;
import java.util.HashMap;
import java.util.Map;

/** Metadata-only Iceberg commits. Never starts Spark or rewrites Iceberg JSON. */
public final class GoldMetadataGuard {
    private static String env(String key) {
        String value = System.getenv(key);
        if (value == null || value.isEmpty()) throw new IllegalArgumentException("Missing " + key);
        return value;
    }
    public static void main(String[] args) throws Exception {
        if (args.length < 2 || !args[0].matches("dbt_gold_[0-9a-f]{24}"))
            throw new IllegalArgumentException("Only isolated dbt work refs are allowed");
        String suffix = args[0].substring(9);
        for (int i = 1; i < args.length; i++) {
            if (!args[i].matches("(teaching_progress_metrics|learning_outcomes_metrics|learning_outcomes_by_period)_" + suffix))
                throw new IllegalArgumentException("Output alias does not belong to work ref");
        }
        Configuration conf = new Configuration();
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.endpoint", env("S3_ENDPOINT"));
        conf.set("fs.s3a.access.key", env("S3_ACCESS_KEY"));
        conf.set("fs.s3a.secret.key", env("S3_SECRET_KEY"));
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.connection.ssl.enabled", Boolean.toString(java.net.URI.create(env("S3_ENDPOINT")).getScheme().equalsIgnoreCase("https")));
        conf.set("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider");
        Map<String, String> options = new HashMap<>();
        options.put("uri", env("NESSIE_API_URL"));
        options.put("ref", args[0]);
        options.put("warehouse", "s3a://" + env("S3_BUCKET_NAME") + "/iceberg-warehouse");
        try (NessieCatalog catalog = new NessieCatalog()) {
            catalog.setConf(conf);
            catalog.initialize("gold_guard", options);
            for (int i = 1; i < args.length; i++) {
                Table table = catalog.loadTable(TableIdentifier.of("gold", args[i]));
                if (!"false".equals(table.properties().get("gc.enabled")) ||
                    !"false".equals(table.properties().get("write.metadata.delete-after-commit.enabled"))) {
                    table.updateProperties().set("gc.enabled", "false")
                        .set("write.metadata.delete-after-commit.enabled", "false").commit();
                }
                table.refresh();
                if (!"false".equals(table.properties().get("gc.enabled")) ||
                    !"false".equals(table.properties().get("write.metadata.delete-after-commit.enabled")))
                    throw new IllegalStateException("GC guard readback failed");
                System.out.println("GUARD_OK=" + args[i]);
            }
        }
    }
}
