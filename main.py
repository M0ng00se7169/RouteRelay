from fastapi import FastAPI

app = FastAPI()


@app.get("/hi")
def greet():
    return "Hello world, motherfucker ;)"


if __name__ == '__main__':
    import uvicorn
    uvicorn.run("main:app", reload=True)
