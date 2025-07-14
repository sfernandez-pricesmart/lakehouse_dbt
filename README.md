Welcome to your new dbt project!

## Prerriquisites
1. Be sure you have python version installed
```bash
python3 --version
```
2. Clone the repo
Go to the folder you want to clone the repo and run

```bash
git clone https://github.com/sfernandez-pricesmart/lakehouse_dbt.git
```


3. Install packages using a virtual environment

```bash
cd cd path/to/your/lakehouse_dbt
python3 -m venv venv
source venv/bin/activate 
pip install dbt-core dbt-snowflake
```

## 👩‍💻 Developer Setup

To run dbt with your sandbox schema:

1. Clone the repo
2. Create a `.env` file in the root:

```env
SANDBOX_USER=yourusername
SANDBOX_SCHEMA=YOURUSERNAME
```

3. Run

```bash
set -a
source .env
set +a
```
test it
```bash
echo $SNOWFLAKE_USER
```

4. Run

```bash
dbt run-operation clone_raw_sources
```


### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
