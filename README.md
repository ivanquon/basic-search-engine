# INF 141 Search Engine
1. Create/Download a sources folder with JSON files each with 1 object following the below format <br/>
` {"url": "URL HERE", "content": "HTML CONTENT HERE", "encoding": "ENCODING HERE"} `<br/>
 Example <br/>
` {"url": "https://www.cs.uci.edu/research-centers/", "content": "<!DOCTYPE html>\n<html lang=\"en-US\">\n<head > . . .", "encoding": "ascii"} `
2. Run indexer.py on source folder (Indexing time may vary based on # of files and length)
3. Run retrieval.py and input search queries
4. (Optional) Open frontend/index.html for a web UI
