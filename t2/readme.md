Para buildar os microserviços do backend e o RabbitMQ, rode:

``` cd backend ```
``` docker-compose up --build ```

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
