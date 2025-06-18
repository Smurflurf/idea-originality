# idea-originality
A tool to analyze an ideas originality using [all-MiniLM-L6-v2](https://huggingface.co/sentence-transformers/all-MiniLM-L6-v2).

# vector database
This project uses a vector database containing entries from several datasets embedded into a vectorspace using the transformer model all-MiniLM-L6-v2.
The used vector db ...

# used datasets
[arXiv](https://www.kaggle.com/datasets/Cornell-University/arxiv) for scientific papers, the abstracts get embedded.
[Github READMEs](https://zenodo.org/records/285419) up to October 2016 (perhaps, not sure yet. READMEs need to be broken down into smaller descriptions before getting added to the db)
[Quoqa Question Dataset](https://www.kaggle.com/datasets/quora/question-pairs-dataset) (perhaps, not sure yet. contains a lot of shallow questions)
Reddit Post Dataset (not quite sure yet on what exactly gets picked, some subreddits and heuristics to discard bad questions)
