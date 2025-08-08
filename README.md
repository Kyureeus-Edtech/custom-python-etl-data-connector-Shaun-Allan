# SSN-college-software-architecture-Assignments-
Assignment repository for building custom Python ETL data connectors (Kyureeus EdTech, SSN CSE). Students: Submit your ETL scripts here. Make sure your commit message includes your name and roll number.
# Software Architecture Assignment: Custom Python ETL Data Connector

Welcome to the official repository for submitting your Software Architecture assignment on building custom data connectors (ETL pipelines) in Python. This assignment is part of the Kyureeus EdTech program for SSN CSE students.

---

## 📋 Assignment Overview

**Goal:**  
Develop a Python script to connect with an API provider, extract data, transform it for compatibility, and load it into a MongoDB collection. Follow secure coding and project structure practices as outlined below.

---

## ✅ Submission Checklist

- [ ] Choose a data provider (API) and understand its documentation
- [ ] Secure all API credentials using a `.env` file
- [ ] Build a complete ETL pipeline: Extract → Transform → Load (into MongoDB)
- [ ] Test and validate your pipeline (handle errors, invalid data, rate limits, etc.)
- [ ] Follow the provided Git project structure
- [ ] Write a clear and descriptive `README.md` in your folder with API details and usage instructions
- [ ] **Include your name and roll number in your commit messages**
- [ ] Push your code to your branch and submit a Pull Request

---

## 📦 Project Structure

/your-branch-name/
├── etl_connector.py
├── .env
├── requirements.txt
├── README.md
└── (any additional scripts or configs)


- **`.env`**: Store sensitive credentials; do **not** commit this file.
- **`etl_connector.py`**: Your main ETL script.
- **`requirements.txt`**: List all Python dependencies.
- **`README.md`**: Instructions for your connector.

---

## 🛡️ Secure Authentication

- Store all API keys/secrets in a local `.env` file.
- Load credentials using the `dotenv` Python library.
- Add `.env` to `.gitignore` before committing.

---

## 🗃️ MongoDB Guidelines

- Use one MongoDB collection per connector (e.g., `connectorname_raw`).
- Store ingestion timestamps for audit and update purposes.

---

## 🧪 Testing & Validation

- Check for invalid responses, empty payloads, rate limits, and connectivity issues.
- Ensure data is correctly inserted into MongoDB.

---

## 📝 Git & Submission Guidelines

1. **Clone the repository** and create your own branch.
2. **Add your code and documentation** in your folder/branch.
3. **Do not commit** your `.env` or secrets.
4. **Write clear commit messages** (include your name and roll number).
5. **Submit a Pull Request** when done.

---

## 💡 Additional Resources

- [python-dotenv Documentation](https://saurabh-kumar.com/python-dotenv/)
- [MongoDB Python Driver (PyMongo)](https://pymongo.readthedocs.io/en/stable/)
- [API Documentation Example](https://restfulapi.net/)

---

## 📢 Need Help?

- Post your queries in the [WhatsApp group](#) or contact your instructor.
- Discuss issues, share progress, and help each other.

---

Happy coding! 🚀
