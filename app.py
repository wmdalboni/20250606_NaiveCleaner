import streamlit as st
import pandas as pd

st.set_page_config(page_title="Dashboard de Tratamento de Dados", layout="wide")

st.title("📊 Dashboard de Tratamento e Profiling de Dados")

st.markdown("""
Faça o upload de um arquivo CSV. O sistema tentará:
1. Identificar e tratar colunas de data.  
2. Identificar e tratar colunas numéricas.  
3. Identificar e tratar colunas de dimensão (categóricas/texto).  
Linhas que não puderem ser convertidas em cada etapa serão armazenadas em “linhas_removidas”.
""")

uploaded_file = st.file_uploader("📁 Selecione o CSV", type=["csv"])

if uploaded_file is not None:
    try:
        # Lê o arquivo CSV e força todas as colunas a serem strings inicialmente
        # Isso evita conversão automática que pode corromper dados para tratamento posterior
        df_original = pd.read_csv(uploaded_file, dtype=str)
    except Exception as e:
        st.error(f"Falha ao ler o CSV: {e}")
        st.stop()

    # Cria uma cópia para manipulação dos dados e um DataFrame para armazenar linhas removidas
    df_tratado = df_original.copy()
    df_removidas = pd.DataFrame(columns=df_original.columns.tolist() + ["Motivo_Tratamento", "Coluna_Alvo", "Valor_Raw"])

    # Lista com todas as colunas para iteração
    cols = df_tratado.columns.tolist()

    # Detecta colunas que contenham termos que indicam IDs ou chaves para forçar tratamento como dimensão (categórica)
    forced_dims = [col for col in cols if any(k in col.lower() for k in ["id", "key", "chave"])]

    # === 1️⃣ Tratamento de Datas ===
    st.subheader("1️⃣ Tratamento de Datas")

    limiar_data = 0.50  # percentual mínimo de sucesso para considerar coluna como data
    date_metric_suffix = "_timestamp"  # sufixo para criar nova métrica de timestamp (epoch em segundos)

    for col in cols:
        if col in forced_dims:
            # Colunas forçadas como dimensão não devem ser convertidas em data
            st.write(f"⚙ Coluna '{col}' forçada como dimensão (ID/key).")
            continue

        # Tenta converter a coluna para datetime, erros geram NaT (valor nulo datetime)
        tentativa = pd.to_datetime(df_tratado[col], errors='coerce', infer_datetime_format=True)
        num_sucesso = tentativa.notna().sum()
        total_linhas = len(df_tratado)

        # Se ao menos 50% das linhas converteram, aceitamos como data
        if num_sucesso / total_linhas >= limiar_data:
            # Substitui a coluna original pela versão convertida
            df_tratado[col] = tentativa

            # Identifica linhas que eram não nulas na original mas falharam na conversão
            mask_nat = df_tratado[col].isna() & df_original.loc[df_tratado.index, col].notna()

            if mask_nat.sum() > 0:
                # Salva essas linhas em df_removidas para análise futura
                failed_rows = df_tratado[mask_nat].copy()
                failed_rows["Motivo_Tratamento"] = "Data inválida - linha removida"
                failed_rows["Coluna_Alvo"] = col
                failed_rows["Valor_Raw"] = df_original.loc[mask_nat, col]
                df_removidas = pd.concat([df_removidas, failed_rows], ignore_index=True)

                # Remove linhas inválidas do df_tratado
                df_tratado = df_tratado[~mask_nat].copy()
                tentativa = tentativa[~mask_nat]

            # Cria nova coluna métrica timestamp com a data convertida para segundos desde epoch
            timestamp_col = col + date_metric_suffix
            df_tratado[timestamp_col] = df_tratado[col].astype('int64') / 1e9

            st.write(f"✅ Coluna '{col}' convertida para datetime. Nova métrica criada: '{timestamp_col}'. Linhas removidas: {mask_nat.sum()}.")
        else:
            # Coluna não é considerada data
            st.write(f"❌ Coluna '{col}' não tratada como data ({num_sucesso}/{total_linhas} conversões).")

    # === 2️⃣ Tratamento de Colunas Numéricas ===
    st.subheader("2️⃣ Tratamento de Colunas Numéricas")

    limiar_num = 0.50  # percentual mínimo para aceitar coluna como numérica
    cols = df_tratado.columns.tolist()  # Atualiza lista de colunas após possíveis remoções

    numeric_cols = []  # Armazena colunas tratadas como numéricas

    for col in cols:
        if col in forced_dims:
            st.write(f"⚙ Coluna '{col}' forçada como dimensão (ID/key).")
            continue

        if col.endswith(date_metric_suffix):
            # Ignora colunas de timestamp criadas no passo anterior para não tentar reconverter
            continue

        # Tenta converter para numérico, valores inválidos viram NaN
        tentativa_num = pd.to_numeric(df_tratado[col], errors='coerce')
        num_sucesso = tentativa_num.notna().sum()
        total_linhas = len(df_tratado)

        if num_sucesso / total_linhas >= limiar_num:
            # Corrige o erro: tenta acessar a coluna original para comparação se existe, para evitar KeyError
            if col in df_original.columns:
                mask_nan = tentativa_num.isna() & df_original.loc[df_tratado.index, col].notna()
            else:
                # Se coluna não existe no df_original, não faz máscara (evita erro)
                mask_nan = pd.Series([False] * len(tentativa_num), index=df_tratado.index)

            if mask_nan.sum() > 0:
                # Salva linhas inválidas em df_removidas
                failed_rows = df_tratado[mask_nan].copy()
                failed_rows["Motivo_Tratamento"] = "Número inválido - linha removida"
                failed_rows["Coluna_Alvo"] = col
                failed_rows["Valor_Raw"] = df_original.loc[df_tratado.index[mask_nan], col]
                df_removidas = pd.concat([df_removidas, failed_rows], ignore_index=True)

                # Remove linhas inválidas do df_tratado
                df_tratado = df_tratado[~mask_nan].copy()
                tentativa_num = pd.to_numeric(df_tratado[col], errors='coerce')

            # Verifica se tem números decimais (float) após remoção de inválidos
            tem_float = (tentativa_num.dropna() % 1 != 0).any()
            if tem_float:
                # Arredonda para 2 casas decimais para evitar excesso de precisão
                df_tratado[col] = tentativa_num.round(2)
            else:
                # Converte para inteiro nullable do pandas para economizar memória e manter NaNs
                df_tratado[col] = tentativa_num.astype('Int64')

            numeric_cols.append(col)
            st.write(f"✅ Coluna '{col}' tratada como numérica ({'float' if tem_float else 'int'}). Linhas removidas: {mask_nan.sum()}.")
        else:
            st.write(f"❌ Coluna '{col}' não tratada como numérica ({num_sucesso}/{total_linhas} conversões).")

    # === 3️⃣ Identificação de Colunas Numéricas Sequenciais ===
    # Em algumas situações, colunas numéricas sequenciais representam IDs e devem ser tratadas como dimensão
    sequential_id_cols = []
    for col in numeric_cols:
        serie = df_tratado[col].dropna()
        try:
            únicos = sorted(serie.astype(int).unique())
            # Verifica se os valores únicos formam uma sequência de 0 até N-1 (sequência contínua)
            if únicos == list(range(len(únicos))):
                sequential_id_cols.append(col)
                st.write(f"⚙ Coluna '{col}' identificada como sequência inteira e tratada como dimensão.")
        except:
            # Caso a conversão para int falhe, ignora
            pass

    # Adiciona essas colunas na lista de dimensões forçadas
    for col in sequential_id_cols:
        if col not in forced_dims:
            forced_dims.append(col)

    # === 4️⃣ Tratamento de Dimensões (Categóricas/Textuais) ===
    st.subheader("3️⃣ Tratamento de Dimensões (Categóricas/Textuais)")

    cols = df_tratado.columns.tolist()
    limite_chars = 12  # Tamanho máximo aceitável para strings em dimensões

    for col in cols:
        # Ignora colunas já convertidas para datetime, colunas numéricas que não são dimensões forçadas e colunas de timestamp
        if (
            pd.api.types.is_datetime64_any_dtype(df_tratado[col])
            or (col in numeric_cols and col not in forced_dims)
            or col.endswith(date_metric_suffix)
        ):
            continue

        # Trata valores nulos como string "Outro" para manter consistência na análise
        col_series = df_tratado[col].copy()
        col_series = col_series.where(col_series.notna(), other=pd.NA)
        col_series = col_series.fillna("Outro")

        # Para cada valor, verifica se o comprimento ultrapassa limite, substitui por "Outro" e registra remoção
        for idx, valor in col_series.items():
            try:
                s = str(valor).strip()
            except Exception:
                # Valor que não é string ou não pode ser convertido, remove linha e registra
                df_removidas = pd.concat([
                    df_removidas,
                    pd.DataFrame([{
                        **df_tratado.loc[idx].to_dict(),
                        "Motivo_Tratamento": "Valor não string - linha removida",
                        "Coluna_Alvo": col,
                        "Valor_Raw": valor
                    }])
                ], ignore_index=True)
                df_tratado = df_tratado.drop(idx)
                continue

            if len(s) > limite_chars:
                # Substitui valor longo por "Outro"
                df_tratado.at[idx, col] = "Outro"

                # Registra linha removida com motivo
                df_removidas = pd.concat([
                    df_removidas,
                    pd.DataFrame([{
                        **df_tratado.loc[idx].to_dict(),
                        "Motivo_Tratamento": f"Texto muito longo (> {limite_chars} caracteres)",
                        "Coluna_Alvo": col,
                        "Valor_Raw": valor
                    }])
                ], ignore_index=True)

        # Garante que NaNs finais são substituídos por "Outro"
        df_tratado[col] = df_tratado[col].fillna("Outro")

        st.write(f"✅ Coluna '{col}' tratada como dimensão.")

    # === Exibição dos resultados ===
    st.subheader("DataFrame tratado")
    st.dataframe(df_tratado)

    st.subheader("Linhas removidas no tratamento")
    st.dataframe(df_removidas)

else:
    st.info("Aguardando upload do arquivo CSV.")
