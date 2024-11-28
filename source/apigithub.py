# Databricks notebook source

# COMMAND ----------
from github import Github
from pyiris.ingestion.config.file_system_config import FileSystemConfig
from pyiris.ingestion.load import FileWriter
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import expr

# Secret salva no AzureKeyvault!
github_token = "github personal access token"

# Nome do workspace no github!
perfil = "Type your github workspace here"

# Lista de repositórios salva na DAG
repositorio = dbutils.widgets.get("repositorio")

# Não modificar o itens, maior valor pode resultar em quebra da operação por ratelimit.
itens = 150

# instancia do spark
spark = SparkSession.builder.appName("GitHub API").getOrCreate()

# Armazena o token do github e utiliza a biblioteca para complementar o endereco de requisicao
g = Github(github_token)

# Salva o workspace e repositorio desejado
repo = g.get_repo(f"{perfil}/{repositorio}")

# cria uma lista de dados vazia para iterar posteriormente
data = []

# Filtro para recuperar apenas os PRs Fechados, em ordem decrecente (do mais novo para o mais velho), apenas os que foram criados e na base master
pulls = repo.get_pulls(state="closed", direction="desc", sort="created", base="master")[
    :itens
]

# Função para encontrar o tempo de aprovação com base nos labels
# labels recebe o nome das labels adicionadas no PR
# merged_at recebe a data em que o PR foi mergeado
# label_time recebe o valor em que a label foi adicionada
# Com base nesses valores é possivel estimar o tempo medio que o time de ops leva para aprovar um PR após a label de aprovado for adicionada

# COMMAND ----------
def calculate_time_to_approve(labels, merged_at, label_time):
    for label in labels:
        if label.name == "": ##Pick a label to filter
            return (merged_at - label_time).total_seconds()
    return None


# Loop que percorre a lista de PRs e extrai as principais informacoes e realiza alguns calculos
for pr in pulls:
    if pr.merged:
        created_at = pr.created_at
        merged_at = pr.merged_at
        labels = pr.get_labels()
        # Captura o tempo em que a label foi adicionada e adiciona a descricao da label a mesma linha para identificar
        label_time = None
        for event in pr.get_issue_events():
            if event.event == "labeled" and event.label.name in [
                label.name for label in labels
            ]:
                label_time = event.created_at
                break
        # Coleta os revisores do PR e filtra pelos reviews com status aprovado.
        approver_str = ",".join(
            review.user.login
            for review in pr.get_reviews()
            if review.state == "APPROVED"
        )
        # Concatena o restante dos dados, como ID do PR e quem o criou
        pr_id = f"#{pr.number}"
        creator = pr.user.login
        # Aqui é atribuido a time_to_approve o calculo feito na Def da linha 30, para calcular o tempo de aprovacao do time de Ops
        time_to_approve = calculate_time_to_approve(labels, merged_at, label_time)
        # Restante das concatenacoes de comentarios, alteracaoes e solicitacoes de review no pr
        num_comments = pr.comments
        num_review_requests = len(list(pr.get_review_requests()[0]))
        num_changes = pr.additions + pr.deletions

        data.append(
            {
                "repository": repositorio,
                "created_at": created_at,
                "merged_at": merged_at,
                "approver": approver_str,
                "pr_id": pr_id,
                "labels": ",".join(label.name for label in labels),
                "label_times": label_time,
                "creator": creator,
                "num_comments": num_comments,
                "num_review_requests": num_review_requests,
                "num_changes": num_changes,
                "SLA": time_to_approve,
            }
        )
# Cria o dataframe
df = spark.createDataFrame(data)

# Calcula o SLA, considerando a diferenca de quando foi mergado - quando foi criado
df = df.withColumn(
    "sla", (F.unix_timestamp("merged_at") - F.unix_timestamp("created_at"))
)

# Calcula se o PR esta aberto a mais de 7 dias e retorna um valor booleano
df = df.withColumn(
    "is_open_for_more_than_7_days", expr("created_at + interval 7 days < merged_at")
)

# Aqui removo as duplicadas
df = df.dropDuplicates()
