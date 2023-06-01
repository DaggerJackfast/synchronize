### Synchronize and anonymize application

#### Start guide:
Node.js version is `v18.15.0`, can see version in `.nvmrc` file. \
Install dependencies
```bash
npm install
```
Setup environment variables file
```bash
cp .env.example .env
```
Set mongo db connection url to `DB_URI` environment variable in `.env` file
#### Run generate customers application
```bash
npm run app
# or
npx ts-node app.ts
```

### Run start synchronize and anonymize
```bash
npm run sync
# or
npx ts-node sync.ts
```

### Run execute synchronize and anonymize with full reindex
```bash
npm run sync -- --full-reindex
# or
npx ts-node sync.ts --full-reindex
```

### Run formatting code
```bash
npm run format
# or
npx prettier --write ./**/*.ts
```