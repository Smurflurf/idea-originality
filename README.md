# idea originality analyzer
A tool to analyze the originality of an idea by mapping it into a high-dimensional vector space and comparing it against a large corpus of existing knowledge. This project utilizes the [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2) sentence-transformer model.

# Core Concept: The Vector Database
This project is built upon a self-hosted vector database that contains entries from several datasets. Those entries are embedded into a 384-dimensional vector space using the `all-MiniLM-L6-v2` transformer model.
For the database technology, this project uses [Qdrant](https://github.com/qdrant/qdrant), a high-performance, open-source vector database, providing a powerful and scalable alternative to commercial services.
A suite of Python scripts is used to populate and manage the database.
Each entry is stored as a `vector : json` pair, where the payload is a JSON object containing two main keys:
-   `type`: A string indicating the source dataset (e.g., "arXiv", "Reddit").
-   `original_json`: The complete, raw JSON object from the source dataset, ensuring no metadata is lost and enabling deep, contextual analysis later on.

# Used datasets
The goal is to build a rich and diverse vector space. The following datasets are being used or considered for ingestion:

## [arXiv](https://www.kaggle.com/datasets/Cornell-University/arxiv) up to June 2025
  > This dataset contains the metadata of 2.760.557 scientific papers, uploaded to [arxiv.org](https://arxiv.org/). It serves as the foundation for complex, scientific, and research-based ideas.

  > Status: currently loading into the DB 
### Embedding into the vector space:
  1. Extract an entries title and abstract.
  2. Sanitize both Strings. They contain formatting artefacts like '\n' and LaTeX formatting. The sanitizing is a rather simple but effective rule-based process. It removes most of the mentioned artefacts (like '\n', '$' or '_') from the String. See `src.py.arxiv_ingest` 
  3. A combined string `"sanitized_title. sanitized_abstract"` is then encoded into a vector using the transformer model.

## [Github READMEs](https://zenodo.org/records/285419) up to October 2016
  > (perhaps, not sure yet. READMEs need to be broken down into smaller descriptions before getting added to the db)
  
  > Status: under consideration
## [Quoqa Question Dataset](https://www.kaggle.com/datasets/quora/question-pairs-dataset) 
  > (perhaps, not sure yet. contains a lot of shallow questions)
  
  > Status: under consideration
## Reddit Post Dataset 
  > (not quite sure yet on what exactly gets picked, some subreddits and heuristics to discard bad questions)
  
  > Status: planned
