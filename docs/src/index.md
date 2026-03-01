---
layout: home.njk
title: Home
---

<section class="hero">
  <h1>qualink</h1>
  <p class="tagline-name" style="color:var(--color-text-secondary);font-size:1.1rem;margin-bottom:8px;letter-spacing:0.5px;"><strong>qual</strong>ity + l<strong>ink</strong> — linking your data to quality.</p>
  <p class="tagline">Blazing fast data quality framework for Python, built on Apache DataFusion.</p>
  <div class="hero-buttons">
    <a href="{{ '/guide/getting-started/' | url }}" class="btn btn-primary">Get Started →</a>
    <a href="{{ '/api/core/' | url }}" class="btn btn-secondary">API Reference</a>
  </div>
  <div class="install-block">
    <span style="color:#8b949e">$</span> uv add qualink
  </div>
</section>

<section class="features-grid">
  <div class="feature-card">
    <div class="icon">🚀</div>
    <h3>High Performance</h3>
    <p>Leverages Apache DataFusion for blazing-fast SQL-based data quality checks with zero-copy Arrow processing.</p>
  </div>
  <div class="feature-card">
    <div class="icon">🔧</div>
    <h3>25+ Built-in Constraints</h3>
    <p>Completeness, uniqueness, statistics, patterns, formats, cross-table comparisons, and more — all ready to use.</p>
  </div>
  <div class="feature-card">
    <div class="icon">📄</div>
    <h3>YAML Configuration</h3>
    <p>Define your entire validation suite declaratively in YAML — no code required for standard checks.</p>
  </div>
  <div class="feature-card">
    <div class="icon">⚡</div>
    <h3>Async First</h3>
    <p>Built with <code>asyncio</code> for non-blocking execution. Run checks sequentially or in parallel.</p>
  </div>
  <div class="feature-card">
    <div class="icon">📊</div>
    <h3>Multiple Formatters</h3>
    <p>Output results as human-readable text, JSON for pipelines, or Markdown for reports.</p>
  </div>
  <div class="feature-card">
    <div class="icon">🖥️</div>
    <h3>CLI – qualinkctl</h3>
    <p>Run validations from the terminal with a single command. Perfect for CI/CD pipelines and automation.</p>
  </div>
  <div class="feature-card">
    <div class="icon">🏗️</div>
    <h3>Fluent Builder API</h3>
    <p>Chain methods to define checks with a clean, readable, Pythonic builder pattern.</p>
  </div>
</section>

<section style="max-width:700px;margin:40px auto 60px;padding:0 24px;">
  <h2 style="text-align:center;color:var(--color-text);margin-bottom:24px;">Quick Example</h2>

<pre><code>import asyncio
from datafusion import SessionContext
from qualink.checks import Check, Level
from qualink.constraints import Assertion
from qualink.core import ValidationSuite
from qualink.formatters import MarkdownFormatter

async def main():
    ctx = SessionContext()
    ctx.register_csv("users", "users.csv")

    result = await (
        ValidationSuite()
        .on_data(ctx, "users")
        .with_name("User Data Quality")
        .add_check(
            Check.builder("Critical")
            .with_level(Level.ERROR)
            .is_complete("user_id")
            .is_unique("email")
            .has_size(Assertion.greater_than(0))
            .build()
        )
        .run()
    )
    print(MarkdownFormatter().format(result))

asyncio.run(main())</code></pre>
</section>

<section style="max-width:800px;margin:0 auto 60px;padding:0 24px;">
  <h2 style="text-align:center;color:var(--color-text);margin-bottom:8px;">⚡ Benchmark Highlights</h2>
  <p style="text-align:center;color:var(--color-text-secondary);margin-bottom:28px;">Real-world validation on NYC Yellow Taxi trip data.</p>

  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));gap:16px;margin-bottom:24px;">
    <div style="text-align:center;padding:20px 12px;background:var(--color-bg-secondary,#161b22);border-radius:10px;">
      <div style="font-size:2rem;font-weight:700;color:var(--color-accent,#58a6ff);">42M</div>
      <div style="font-size:0.85rem;color:var(--color-text-secondary);">Records</div>
    </div>
    <div style="text-align:center;padding:20px 12px;background:var(--color-bg-secondary,#161b22);border-radius:10px;">
      <div style="font-size:2rem;font-weight:700;color:var(--color-accent,#58a6ff);">654 MB</div>
      <div style="font-size:0.85rem;color:var(--color-text-secondary);">Parquet Data</div>
    </div>
    <div style="text-align:center;padding:20px 12px;background:var(--color-bg-secondary,#161b22);border-radius:10px;">
      <div style="font-size:2rem;font-weight:700;color:var(--color-accent,#58a6ff);">92</div>
      <div style="font-size:0.85rem;color:var(--color-text-secondary);">Constraints</div>
    </div>
    <div style="text-align:center;padding:20px 12px;background:var(--color-bg-secondary,#161b22);border-radius:10px;">
      <div style="font-size:2rem;font-weight:700;color:var(--color-accent,#58a6ff);">1.44s</div>
      <div style="font-size:0.85rem;color:var(--color-text-secondary);">Engine Time</div>
    </div>
  </div>

  <p style="text-align:center;color:var(--color-text-secondary);font-size:0.9rem;">
    12 check groups · 98.9% pass rate · powered by Apache DataFusion<br>
    <a href="{{ '/guide/benchmarks/' | url }}" style="color:var(--color-accent,#58a6ff);">See full benchmark details →</a>
  </p>
</section>

<section style="max-width:800px;margin:0 auto 60px;padding:0 24px;">
  <h2 style="text-align:center;color:var(--color-text);margin-bottom:8px;">🗺️ Upcoming Features</h2>
  <p style="text-align:center;color:var(--color-text-secondary);margin-bottom:28px;">Here's what's on the roadmap.</p>

  <div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;">
    <div style="padding:20px;background:var(--color-bg-secondary,#161b22);border-radius:10px;border-left:3px solid var(--color-accent,#58a6ff);">
      <h4 style="margin:0 0 6px;color:var(--color-text);">📈 Analyzers</h4>
      <p style="margin:0;font-size:0.85rem;color:var(--color-text-secondary);">Automatically profile your data — distributions, cardinality, null rates — before writing a single rule.</p>
    </div>
    <div style="padding:20px;background:var(--color-bg-secondary,#161b22);border-radius:10px;border-left:3px solid var(--color-accent,#58a6ff);">
      <h4 style="margin:0 0 6px;color:var(--color-text);">🗄️ Metrics Repository</h4>
      <p style="margin:0;font-size:0.85rem;color:var(--color-text-secondary);">Persist validation metrics over time to track data quality trends, regressions, and SLA compliance.</p>
    </div>
    <div style="padding:20px;background:var(--color-bg-secondary,#161b22);border-radius:10px;border-left:3px solid var(--color-accent,#58a6ff);">
      <h4 style="margin:0 0 6px;color:var(--color-text);">🔍 Anomaly Detection</h4>
      <p style="margin:0;font-size:0.85rem;color:var(--color-text-secondary);">Detect unexpected shifts in data distributions and metric values using statistical methods.</p>
    </div>
    <div style="padding:20px;background:var(--color-bg-secondary,#161b22);border-radius:10px;border-left:3px solid var(--color-accent,#58a6ff);">
      <h4 style="margin:0 0 6px;color:var(--color-text);">💡 Intelligent Rule Suggestions</h4>
      <p style="margin:0;font-size:0.85rem;color:var(--color-text-secondary);">Get automatic constraint recommendations based on data profiling — jump-start your validation suite.</p>
    </div>
  </div>
</section>
