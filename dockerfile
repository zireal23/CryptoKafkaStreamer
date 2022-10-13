#Builder stage 
FROM golang:1.18.4-alpine AS builder
WORKDIR /app
COPY . .
RUN apk add build-base
RUN go build -tags musl -o main main.go 


#RUN stage
FROM alpine 
WORKDIR /app
COPY --from=builder /app/main .
CMD ["/app/main"]
ENV MONGODB_URI mongodb+srv://cryptoDB:sayan@cluster0.dve8ojg.mongodb.net/?retryWrites=true&w=majority
ENV BOOSTRAP_SERVERS pkc-41p56.asia-south1.gcp.confluent.cloud:9092
ENV SASL_USERNAME 2QWGO7JGYQJ5ZMGZ
ENV SASL_PASSWORD JNt+0blpAMoWFSf5sbkgdAzvE2ty+8uVmBK22WJBqPJJtA2dMdR5bpcItZpAzRHd
ENV TOPIC cryptoPrices