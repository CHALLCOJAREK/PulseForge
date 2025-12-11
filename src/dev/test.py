from src.pipelines.pipeline_matcher import PipelineMatcher

pm = PipelineMatcher()
df_match = pm.run()

print("\n=== COLUMNAS ===")
print(df_match.columns)

print("\n=== HEAD ===")
print(df_match.head(10))
