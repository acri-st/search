# Search

📌 [DESP-AAS Collaborative Services Parent Repository](https://github.com/acri-st/DESP-AAS-Collaborative-Services)

## Table of Contents

- [Introduction](#Introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Development](#development)
- [Contributing](#contributing)

## Introduction

### What is the Search microservice?

The Search microservice is a specialized service designed to provide advanced search capabilities for assets within the platform. It leverages Elasticsearch to deliver fast, scalable, and intelligent search functionality.

**Key Features:**
- **Full-text search** across asset metadata and content
- **Geographic search** with support for GeoJSON shapes and coordinate reference systems
- **Asset categorization** and filtering by type, source, and category
- **Scoring and ranking** based on relevance, popularity (likes, downloads, views), and user preferences
- **Real-time indexing** of new assets and metadata updates

**Supported Asset Types:**
- User-generated content
- System-generated assets
- Various document types with customizable metadata

**Technical Capabilities:**
- RESTful API endpoints for search operations
- Configurable search scoring algorithms
- Support for complex geographic queries
- Subscription-based access control


## Prerequisites

Before you begin, ensure you have the following installed:
- **Git** 
- **Docker** Docker is mainly used for the test suite, but can also be used to deploy the project via docker compose
- **Rabbitmq** to send mail event in a queue

## Installation

1. Clone the repository:
```bash
git clone https://github.com/acri-st/moderation-handling.git moderation_handling
cd moderation_handling
```

## Development

## Development Mode

### Standard local development

Setup environment
```bash
make setup
```

To clean the project and remove node_modules and other generated files, use:
```bash
make clean
```

## Contributing

Check out the **CONTRIBUTING.md** for more details on how to contribute.
