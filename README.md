# Idea Originality Analyzer
This repository contains the full-stack application for the "Idea Originality Analyzer", a tool to analyze the originality of an idea by mapping it into a high-dimensional vector space and comparing it against a large corpus of existing knowledge. This project utilizes the [e5-base-4k](https://huggingface.co/dwzhu/e5-base-4k) sentence-transformer model.

# Core Concept: The Vector Database
This project is built upon a self-hosted vector database that contains entries from several datasets. Those entries are embedded into a 768-dimensional vector space using the `e5-base-4k` transformer model.
For the database technology, this project uses [Qdrant](https://github.com/qdrant/qdrant), a high-performance, open-source vector database, providing a powerful and scalable alternative to commercial services.
A suite of Python scripts is used to populate and manage the database.
Each entry is stored as a `vector : json` pair, where the payload is a JSON object containing two main keys:
-   `type`: A string indicating the source dataset (e.g., "arXiv", "PhilPapers").
-   `original_json`: The complete, raw JSON object from the source dataset, ensuring no metadata is lost and enabling deep, contextual analysis later on.

# The Analyzer Application
A sophisticated web application serves as the user interface to the vector database. It is designed to be intuitive, responsive, and to provide clear, actionable insights from the complex backend processes.

## Architecture Overview
The system is a modern full-stack application composed of three main components that communicate asynchronously:
-   **Backend (Java Spring Boot):** The central orchestrator of the application.
-   **Python Services:** A dedicated, Java-managed Python process for heavy AI tasks (vectorization).
-   **Frontend (Web UI):** A dynamic user interface built with HTML, CSS, and modern JavaScript.

## Backend (Java Spring Boot)
The backend is the application's core logic. Its key responsibilities include:
-   Serving the web application and providing a REST API for queries.
-   Managing the entire analysis lifecycle asynchronously, from user input to final results.
-   Communicating directly with the Qdrant database via gRPC.
-   Managing the lifecycle of the external Python vectorizer service, including a robust, event-driven startup sequence.
-   Performing server-side data sanitization and conversion of LaTeX fragments to clean Unicode using a combination of the **JLaTeXMath** and **SnuggleTeX** library before rendering.

## Frontend (Web UI)
The user-facing part of the project is a single-page-style application designed for a seamless experience. Key features include:
-   **Multi-Modal Input:** Users can describe their idea not just with text, but also by uploading supporting files (images, PDFs) or by recording audio directly in the browser.
-   **Real-Time Feedback:** After submitting a query, a progress popup appears, providing real-time status updates from the backend via Server-Sent Events (SSE).
-   **Interactive Results:** The results page presents a clean summary of the most similar items and offers a collapsible view to inspect the raw JSON data.

# Used datasets
The goal is to build a rich and diverse vector space. The following datasets are being used or considered for ingestion:

## [arXiv](https://www.kaggle.com/datasets/Cornell-University/arxiv) up to June 2025
  > This dataset contains the metadata of 2.760.557 scientific papers, uploaded to [arxiv.org](https://arxiv.org/). It serves as the foundation for complex, scientific, and research-based ideas.

  > Status: finished embedding into the db 
### Embedding into the vector space:
  1. Extract an entries title and abstract.
  2. Sanitize both Strings. They contain formatting artefacts like '\n' and LaTeX formatting. The sanitizing is a rather simple but effective rule-based process. It removes most of the mentioned artefacts (like '\n', '$' or '_') from the String. See `src.py.arxiv_ingest` 
  3. A combined string `"sanitized_title. sanitized_abstract"` is then encoded into a vector using the transformer model.

## [PhilPapers](https://philpapers.org/)
  > This dataset contains 2,740,194 philosophical entries. It serves as the foundation for complex, philosophical ideas.

  > Status: under heavy consideration, will be the next dataset for the db

## [RePEc (Research Papers in Economics)](http://repec.org/)
  > This dataset contains about 5 million research items from 4,200 journals and 5,600 working paper series. It serves as the foundation for complex, economic ideas.

  > Status: under heavy consideration, will be added shortly after PhilPapers

## [OpenAlex](https://help.openalex.org/hc/en-us/articles/24396686889751-About-us)
  > This dataset contains over 250M scholarly works from 250k sources. Its enourmous size replaces the need for other scientific datasets but makes it not feasible to use right now. Will be added when I scaled my infrastructure to fit it, but this might take quite a while.

  > Status: under heavy consideration, but my infrastructure cannot support it right now. Will be added when that's the case.

## [Github READMEs](https://zenodo.org/records/285419) up to October 2016
  > (perhaps, not sure yet. READMEs need to be broken down into smaller descriptions before getting added to the db)
  
  > Status: under consideration
## [Quoqa Question Dataset](https://www.kaggle.com/datasets/quora/question-pairs-dataset) 
  > (perhaps, not sure yet. contains a lot of shallow questions)
  
  > Status: under consideration
## Reddit Post Dataset 
  > (not quite sure yet on what exactly gets picked, some subreddits and heuristics to discard bad questions)
  
  > Status: planned
