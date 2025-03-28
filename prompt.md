📘 AI Project Instructions (prompt.md)

This file contains development rules and best practices for building a data engineering project focused on macroeconomic and financial market data using modern open-source tooling. This file is designed to guide both AI systems (like ChatGPT and Cursor) and human contributors.

⸻

🧠 Project Context
	•	Goal: Build a personal data pipeline for macroeconomic data, financial markets (stocks, crypto, commodities, futures, options)
	•	Output Uses: Investment analytics, automated blog generation, internal dashboards
	•	Written In: Python (using Cursor)
	•	Powered By: AI-assisted code generation (e.g., ChatGPT)

⸻

✅ General Development Rules
	1.	Follow Medallion Architecture
	•	Bronze = Raw ingestion
	•	Silver = Cleaned & validated
	•	Gold = Aggregated/analytics-ready
	2.	Default Tech Stack:
	•	Language: Python
	•	Storage: ClickHouse, Apache Iceberg
	•	Infra: Docker, Docker Compose, AWS (S3, EC2, ECR), MinIO (alt to S3), Kubernetes (later phase)
	•	Workflow: Apache Airflow
	•	Web/App: React + TailwindCSS, Streamlit, Gradio (for internal tools)
	3.	Code Practices:
	•	Modular and AI-generatable code
	•	Always include logging, config support (.env/YAML/JSON), data validation
	•	CLI or Notebook interfaces to test pipeline stages
	•	Support local and cloud modes
	4.	All code/tasks must be given in 3 styles:
	•	✅ Best Practice — clean, scalable, prod-ready
	•	🛠️ Practical — simple, fast, local-friendly
	•	💡 Creative — experimental or cutting-edge
	5.	Prefer open-source tools unless explicitly asked to use cloud services.

⸻

🧰 Cursor-Specific Instructions
	6.	Cursor Rules and MCP Config:
	•	When relevant to a task, provide optimized cursor.rules or mcp suggestions
	•	Tailor rules to support smart file routing, formatting, and autocomplete
	•	Assume all code will be written and tested inside Cursor

⸻

📈 Pipeline Rules
	7.	Ingestion Scripts Should:
	•	Separate concerns (download, load, transform)
	•	Include basic schema validation, null/missing detection
	•	Be fully logged and config-driven
	8.	ClickHouse Usage:
	•	Use native formats (Parquet, ORC) for bulk loads
	•	Include table DDL, schema, indexes
	•	Optimize inserts for performance (batching, partitioning)

⸻

🌐 Web & App Phase (Later)
	9.	Frontend Tools:
	•	React or Next.js + TailwindCSS
	•	Use Streamlit/Gradio for internal dashboards or quick POCs
	10.	Blog Generation:

	•	Markdown or static site generation with templated insights
	•	Possibly integrate with Notion, GitHub Pages, or a JAMstack CMS

⸻

✨ Final Guidelines
	•	Keep everything modular and AI-friendly
	•	Use clean naming conventions and docstrings
	•	Ask for clarification before making assumptions
	•	Include dev vs prod config toggles where needed

⸻

This file can be referenced by any LLM agents, collaborators, or AI-based tooling to align contributions with the project’s architecture, tech stack, and goals.