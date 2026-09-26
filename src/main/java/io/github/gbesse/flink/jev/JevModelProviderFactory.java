package io.github.gbesse.flink.jev;

import java.util.Set;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.ModelProviderFactory;
import org.apache.flink.table.functions.AsyncPredictFunction;
import org.apache.flink.table.ml.AsyncPredictRuntimeProvider;
import org.apache.flink.table.ml.ModelProvider;

/** Flink SQL model provider for bounded Jev decisions. */
public final class JevModelProviderFactory implements ModelProviderFactory {
  public static final ConfigOption<String> ENDPOINT = ConfigOptions.key("endpoint").stringType()
      .defaultValue("https://api.typesafe.ai/v1/systemone");
  public static final ConfigOption<String> MODEL = ConfigOptions.key("model").stringType()
      .defaultValue("jev-1.13.0");
  public static final ConfigOption<String> QUESTION = ConfigOptions.key("question").stringType().noDefaultValue();
  public static final ConfigOption<Double> THRESHOLD = ConfigOptions.key("threshold").doubleType().defaultValue(0.8);
  public static final ConfigOption<Integer> MAX_INPUT_BYTES = ConfigOptions.key("max-input-bytes").intType().defaultValue(32768);
  public static final ConfigOption<Integer> TIMEOUT_MS = ConfigOptions.key("timeout-ms").intType().defaultValue(10000);
  public static final ConfigOption<Integer> MAX_CALLS = ConfigOptions.key("max-calls-per-task").intType().defaultValue(10000);

  @Override public String factoryIdentifier() { return "jev"; }
  @Override public Set<ConfigOption<?>> requiredOptions() { return Set.of(QUESTION); }
  @Override public Set<ConfigOption<?>> optionalOptions() {
    return Set.of(ENDPOINT, MODEL, THRESHOLD, MAX_INPUT_BYTES, TIMEOUT_MS, MAX_CALLS);
  }

  @Override
  public ModelProvider createModelProvider(Context context) {
    FactoryUtil.ModelProviderFactoryHelper helper = FactoryUtil.createModelProviderFactoryHelper(this, context);
    helper.validate();
    ReadableConfig config = helper.getOptions();
    JevSettings settings = new JevSettings(config.get(ENDPOINT), config.get(MODEL),
        config.get(QUESTION), config.get(THRESHOLD), config.get(MAX_INPUT_BYTES),
        config.get(TIMEOUT_MS), config.get(MAX_CALLS));
    settings.validate();
    return new Provider(settings);
  }

  private static final class Provider implements AsyncPredictRuntimeProvider {
    private final JevSettings settings;
    private Provider(JevSettings settings) { this.settings = settings; }
    @Override public AsyncPredictFunction createAsyncPredictFunction(ModelProvider.Context context) {
      return new JevPredictFunction(settings);
    }
    @Override public ModelProvider copy() { return new Provider(settings); }
  }
}
