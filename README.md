# trend-stock-scanner-app

This is a small [Streamlit](https://streamlit.io) demo app for a stock
scanner that works based on finding trends in the market.

See it [here](https://stockscan.streamlit.app).



<img width="1334" alt="Screenshot 2023-07-17 at 1 25 50 PM" src="https://github.com/GCristianD/StockScan/assets/26828604/f1674a64-87b2-4063-8f3d-598e57fa0fcc">



<img width="1335" alt="Screenshot 2023-07-17 at 1 25 58 PM" src="https://github.com/GCristianD/StockScan/assets/26828604/0ab78f54-50c4-4fa2-9fec-39da443801de">













## Development

We use [Poetry](https://python-poetry.org), [Flake8](https://flake8.pycqa.org/en/latest/),
[Black](https://github.com/psf/black) and [MyPy](https://www.mypy-lang.org) to
keep our code in check. [Pytest](https://docs.pytest.org/en/7.3.x/) is our go-to
framework for writing automated tests. We host and run this app using [Streamlit](https://streamlit.io).

A development environment setup goes something like this:

1. Install Python using the official installer. If you want, you
   can give [pyenv](https://github.com/pyenv/pyenv) and the
   [pyenv-virtualenv](https://github.com/pyenv/pyenv-virtualenv) plugin a try.
2. Install Poetry using the official installer system-wide.
3. In the directory where this `README.md` file is located, run

   ```bash 
   $ poetry install
   ```
   
   This will create a virtualenv for you (or it will reuse the virtualenv you've
   created using `pyenv virtualenv`).
4. Next, create a `.streamlit/secrets.toml` file that contains the secrets
   used by the app to connect to various dependencies.
5. Finally, run
   ```bash
   $ poetry run streamlit run src/trend_stock_scanner_app/New.py
   ```
