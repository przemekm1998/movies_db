# movies_db

The task consisted of writing a script that fills example data source(SQLite DB) with data from OMDb API. Given data source contains only titles of movies.

## Available functionalities

* Fill the empty titles with data

  `python movies.py --update`

* Sorting movies by every column

  `python movies.py --sort_by year`


* Filtering movies

  `python movies.py --filter_by language spanish`


* Comparing:

  `python movies.py --compare runtime "Seven Pounds" "Memento"`

* Adding movies to data source

  `python movies.py --add "Kac Wawa"`

* Sowing current highscores

  `python movies.py --highscores`

* If you want your results in .csv file, just add --write_csv

  `python movies.py --highscores --write_csv`

## TODO:
* Refactor the code and tests
