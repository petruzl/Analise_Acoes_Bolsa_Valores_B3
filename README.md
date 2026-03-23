# 📊 Análise Fundamentalista - LPA / VPA (CVM + Mercado)

Projeto de análise fundamentalista automatizada utilizando dados da CVM e integração com mercado via Yahoo Finance.


---

## 🚀 Objetivo

Calcular indicadores financeiros essenciais:

- LPA (Lucro por Ação)
- VPA (Valor Patrimonial por Ação)
- P/L
- P/VP

---

## 🧠 Tecnologias

- Python
- Pandas
- NumPy
- yfinance
- OpenPyXL

---

## 📂 Estrutura
Analise_Acoes_Bolsa_Valores_B3/
│
├── data/
│   ├── raw/          # arquivos CVM
│   ├── processed/    # dados tratados
│
├── output/
│   └── LPA_VPA.xlsx
│
├── src/
│   ├── extract.py
│   ├── transform.py
│   ├── load.py
│   ├── market.py
│
├── main.py
├── requirements.txt
└── README.md
---

## ⚙️ Pipeline

1. Extração → leitura de arquivos CVM
2. Transformação → cálculo LPA e VPA
3. Enriquecimento → dados de mercado
4. Output → Excel final

---

## 📊 Exemplo de saída

| Empresa | Preço | LPA | VPA | P/L | P/VP |
|--------|------|-----|-----|-----|------|
| VALE3 | 75.81 | X | X | X | X |

---

## 🖥️ Como rodar

```bash
pip install -r requirements.txt
python main.py

---

## 📬 Contato

📧 Email: leandro.petruz@gmail.com

📱 WhatsApp: (19) 99590-2992
💼 LinkedIn: https://www.linkedin.com/in/leandro-petruz-0208b84b

🌐 Portfólio: https://petruzl.github.io/
