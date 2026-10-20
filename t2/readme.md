Para rodar o RabbitMQ:

``` cd backend ```
``` docker-compose up -d ```

Para rodar o frontend:

```bash
cd frontend
npm run dev
```

Para rodar backend de pagamentos:

```bash
python3 -m uvicorn payment_api.main:app --reload --port 8001
```

```bash
cd backend
python3 -m uvicorn pagamento.main:app --reload --port 8002
```

Para rodar backend de estoque:

```bash
cd backend
python3 -m uvicorn estoque.main:app --port 8003
```

Para rodar backend principal:

```bash
cd backend
python3 -m uvicorn principal.main:app --port 8000
```

Para rodar backend entrega:

```bash
cd backend
python3 -m entrega.main
```
