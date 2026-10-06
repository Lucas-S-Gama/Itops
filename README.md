# ITOPS – Pipeline de dados (Bronze → Silver → Gold) na AWS

Projeto da disciplina de Sistemas Operacionais em Nuvem (São Paulo Tech School).

| Camada | Prefixo | Conteúdo | Gerado por |
|---|---|---|---|
| Bronze | `01-bronze/` | JSON brutos de firewall (`fw01`) e APs (`ap01`–`ap03`) | script de captura (local) |
| Silver | `02-silver/` | `dados_consolidados.csv` (vazão em Mbps, status de carga, correlação AP × firewall) | `bronze2silver.py` |
| Gold | `03-gold/` | `ranking_antenas.csv` e `mapa_calor_logistico.csv` | `silver2gold.py` |

## Pré-requisitos

- Conta AWS com permissão para S3 e EC2 (nos testes, um laboratório AWS com a função `LabInstanceProfile`).
- Python 3 e Git Bash (ou outro terminal) na máquina local.
- Arquivo de chave `.pem` do par de chaves da EC2.
- Os scripts do projeto: `CapturaFirewall.py`, `bronze2silver.py` e `silver2gold.py`.

> Os comandos abaixo usam `itops-gama` como nome do bucket. Se os scripts tiverem outro nome fixo no código, use o mesmo nome aqui. Nomes de bucket são únicos no mundo todo, então talvez você precise escolher outro.

---

## Parte 1 – Máquina local

### 1. Criar e ativar o ambiente virtual

```bash
cd /c/Users/<usuario>/Documents/<pasta-do-projeto>/itops
python -m venv itops
source itops/Scripts/activate        # Git Bash no Windows
# Linux/macOS: source itops/bin/activate
```

### 2. Instalar as dependências

```bash
pip install pandas psutil boto3
```

> Ajuste a lista conforme os `import` do seu script de captura.

### 3. Configurar as credenciais da AWS

Crie o arquivo `~/.aws/credentials` com as credenciais da sua conta:

```ini
[default]
aws_access_key_id = <SUA_ACCESS_KEY>
aws_secret_access_key = <SEU_SECRET>
aws_session_token = <SEU_TOKEN>
```

- O `aws_session_token` só existe em credenciais temporárias (laboratórios, SSO). Elas **expiram**, então atualize o arquivo a cada nova sessão.
- **Nunca** coloque esses valores no código, no Git ou em prints de documentação.

---

## Parte 2 – Amazon S3

### 4. Criar o bucket e os prefixos

Pelo console (S3 → Criar bucket) ou pelo AWS CLI:

```bash
aws s3 mb s3://itops-gama --region us-east-1

aws s3api put-object --bucket itops-gama --key 01-bronze/
aws s3api put-object --bucket itops-gama --key 02-silver/
aws s3api put-object --bucket itops-gama --key 03-gold/
```

Confirme com `aws s3 ls s3://itops-gama`.

---

## Parte 3 – Instância EC2

### 5. Criar a instância

No console: EC2 → **Executar instância**.

| Campo | Valor usado |
|---|---|
| Nome | `itops-vm` |
| AMI | Ubuntu (Canonical, amd64) |
| Tipo | `t3.small` |
| Armazenamento | 1 volume de 8 GiB |
| Par de chaves | o seu `.pem` |
| Grupo de segurança | porta **22 (SSH)** liberada para o seu IP |

Depois de criada, associe a função do IAM: EC2 → selecione a instância → **Ações → Segurança → Modificar função do IAM** → `LabInstanceProfile`. Com isso, a EC2 acessa o S3 sem chaves no código.

### 6. Pegar o endereço público

No console, copie o **IPv4 público** (ou o DNS público) da instância. Ele será usado nos comandos `ssh` e `scp`.

> O IP privado (`172.x.x.x`, o mesmo que aparece no prompt da EC2) **não** funciona fora da VPC. O IP público também muda se você parar e iniciar a instância, a menos que use um Elastic IP.

### 7. Conectar por SSH

```bash
chmod 400 minha-chave.pem
ssh -i minha-chave.pem ubuntu@<IP_PUBLICO>
```

### 8. Instalar o AWS CLI v2 (dentro da EC2)

```bash
sudo apt update && sudo apt install -y unzip
curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"
unzip awscliv2.zip
sudo ./aws/install
aws --version
```

Teste o acesso ao S3 pela função do IAM:

```bash
aws s3 ls
```

O bucket `itops-gama` deve aparecer na lista.

---

## Parte 4 – Executar o pipeline

### 9. Bronze: capturar e enviar os dados (máquina local)

Com o ambiente virtual `itops` ativo:

```bash
python CapturaFirewall.py
```

O script roda em loop (intervalo de 60 s), mostra mensagens no terminal e grava arquivos como `2026-10-06_17-19_fw01.json` em `s3://itops-gama/01-bronze/`. Pressione `Ctrl+C` para encerrar.

Para conferir, na EC2 ou na máquina local:

```bash
aws s3 ls s3://itops-gama --recursive --human-readable
```

### 10. Sincronizar o bronze com a EC2 (dentro da EC2)

```bash
aws s3 sync s3://itops-gama/01-bronze/ /home/ubuntu/01-bronze
ls 01-bronze
```

Esperado: arquivos do firewall (`fw01`) e dos APs (`ap01`, `ap02`, `ap03`).

### 11. Enviar os scripts para a EC2 (máquina local)

O Git Bash geralmente não tem `rsync`, então use o `scp`. Em caminhos com espaço, use aspas.

```bash
scp -i minha-chave.pem "/c/Users/<usuario>/Documents/<pasta>/bronze2silver.py" ubuntu@<IP_PUBLICO>:/home/ubuntu/itops-python/
scp -i minha-chave.pem "/c/Users/<usuario>/Documents/<pasta>/silver2gold.py"   ubuntu@<IP_PUBLICO>:/home/ubuntu/itops-python/
```

Se a pasta de destino não existir, crie antes na EC2: `mkdir -p ~/itops-python`.

Na primeira conexão, digite `yes` quando perguntar se confia no host.

### 12. Preparar o Python na EC2

```bash
cd ~/itops-python
sudo apt install -y python3-venv python3-pip     # se o venv não estiver disponível
python3 -m venv venv
source venv/bin/activate
pip install pandas boto3
```

### 13. Silver: consolidar os dados

```bash
python3 bronze2silver.py
```

O script:

1. lê os JSON de `01-bronze/` (APs e firewall);
2. calcula a vazão em Mbps entre coletas consecutivas (resets de contador são sinalizados e não entram no cálculo);
3. classifica o status de carga;
4. correlaciona o tráfego das APs com o do firewall, minuto a minuto;
5. grava `s3://itops-gama/02-silver/dados_consolidados.csv`.

Ele repete a consolidação a cada 180 s. Use `Ctrl+C` para encerrar.

Para baixar e conferir o resultado:

```bash
aws s3 sync s3://itops-gama/02-silver/ /home/ubuntu/02-silver
ls 02-silver
```

### 14. Gold: gerar as tabelas de análise

```bash
python3 silver2gold.py
```

Saída esperada: leitura do `dados_consolidados.csv` e criação de `mapa_calor_logistico.csv` e `ranking_antenas.csv` em `s3://itops-gama/03-gold/`.

```bash
ls 03-gold                              # se tiver sincronizado a pasta
aws s3 ls s3://itops-gama/03-gold/
aws s3 cp s3://itops-gama/03-gold/ranking_antenas.csv - 
```

Exemplo de resultado do `ranking_antenas.csv`:

| Ranking | ID | Setor | Volume_Total_MB | Pct_do_Trafego_Total |
|---|---|---|---|---|
| 1 | ap03 | Setor C | 49,16 | 44,73% |
| 2 | ap01 | Setor A | 36,82 | 33,50% |
| 3 | ap02 | Setor B | 23,92 | 21,76% |

Os valores variam conforme os dados capturados.

---

## Problemas comuns

| Erro | Causa | Solução |
|---|---|---|
| `rsync: command not found` | Git Bash não inclui o rsync | Usar `scp` |
| `Could not resolve hostname 172-31-...` | Foi usado o IP privado | Usar o IPv4/DNS **público** da instância |
| `Identity file ... not accessible` | Nome ou caminho da chave incorreto | Conferir com `ls` e usar o nome exato do `.pem` |
| `UNPROTECTED PRIVATE KEY FILE` | Permissão da chave muito aberta | `chmod 400 minha-chave.pem` |
| `Connection timed out` | Porta 22 bloqueada ou IP mudou | Liberar a porta 22 para o seu IP no grupo de segurança |
| `ExpiredToken` / `InvalidAccessKeyId` | Credenciais temporárias expiraram | Atualizar o `~/.aws/credentials` |
| `Unable to locate credentials` na EC2 | Função do IAM não associada | Associar `LabInstanceProfile` à instância |
| `ModuleNotFoundError` | Dependência não instalada no venv | Ativar o venv e rodar `pip install <pacote>` |

## Boas práticas

- Não versione chaves `.pem` nem o `~/.aws/credentials`. Adicione-os ao `.gitignore`.
- Encerre ou pare a EC2 quando não estiver usando, para evitar custos.
- Para rodar o `bronze2silver.py` continuamente em segundo plano, use `nohup python3 bronze2silver.py > saida.log 2>&1 &` ou um serviço `systemd`.