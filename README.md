# miniinfra

Infrastructure from scratch, in Go.

We started miniinfra to bring the best ideas from mature infrastructure into smaller, independent systems. We want each one to hold up under load and stay straightforward to run and maintain.

We call it *mini* because we keep each project focused on one job. We start with the data structures and algorithms that job depends on, keep the implementation clear and efficient, and add new capabilities when they serve the same purpose.

## Projects

- [**minidb**](minidb/README.md) is a relational database for transactional applications.
- [**minikv**](minikv/README.md) is an in-memory key-value store for low-latency access to application data.
- [**minimq**](minimq/README.md) is a message queue for reliable, asynchronous communication between services.
