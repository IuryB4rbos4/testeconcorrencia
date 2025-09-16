#!/bin/bash
set -e

# Diretório para o dataset
DATA_DIR=/app/dataset
mkdir -p $DATA_DIR

# Verifica se kaggle.json existe
if [ ! -f /root/.kaggle/kaggle.json ]; then
  echo "Erro: kaggle.json não encontrado em /root/.kaggle/"
  exit 1
fi

# Baixa o dataset se ainda não estiver presente
if [ ! -f "$DATA_DIR/brazilian-ecommerce.zip" ]; then
    echo "Baixando dataset do Kaggle..."
    kaggle datasets download olistbr/brazilian-ecommerce -p $DATA_DIR
    unzip -o "$DATA_DIR/brazilian-ecommerce.zip" -d $DATA_DIR
else
    echo "Dataset já existe em $DATA_DIR, pulando download."
fi

# Executa o ETL
python etl.py
