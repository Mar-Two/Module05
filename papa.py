if __name__ == "__main__":
    s = {"sensor": "temp", "value": 33.5, "unit": "C"}
    if "sensor" in s and "value" in s and "unit" in s:
        print("True")
    else:
        print(False)