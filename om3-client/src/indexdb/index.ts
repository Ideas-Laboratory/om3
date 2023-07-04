
let isOpen = false

function base64StringToArray(str: string) {
    // 假设要存储的 Base64 数据为 base64String
    const byteCharacters = atob(str);
    const byteNumbers = new Array(byteCharacters.length);
    for (let i = 0; i < byteCharacters.length; i++) {
        byteNumbers[i] = byteCharacters.charCodeAt(i);
    }
    const byteArray = new Uint8Array(byteNumbers);
    return byteArray
}

let indexDb: any = null

export function initIndexDB() {
    const request = indexedDB.open("om_flag_db", 1);

    request.onupgradeneeded = function (event) {
        //@ts-ignore
        const db = event.target.result
        const objectStore = db.createObjectStore("om3_flag", { keyPath: "url" });
    }
    return new Promise((resolve, rej) => {
        request.onerror = function (event) {
            console.log(event);
            console.error("indexdb error, which will case flag cannot store in local",)
            rej("")
        }
        request.onsuccess = function (event) {
            //@ts-ignore
            const db = event.target.result;
            indexDb = db;
            isOpen = true
            resolve('');

            // 在这里进行后续操作，例如读取或写入数据
        };
    })

}

export function getIndexDB() {
    return indexDb
}

export function indexPutData(url: string, str: string) {
    if(!isOpen){
        indexDb()
        return
    }

    const transaction = indexDb.transaction(["om3_flag"], "readwrite");
    const objectStore = transaction.objectStore("om3_flag");
    const dta = { url: url, binaryData: base64StringToArray(str) }
    objectStore.put(dta);
}

export function indexGetData(url: string) {
    if(!isOpen){
        initIndexDB()
        return false
    }

    const transaction = indexDb.transaction(["om3_flag"], "readonly");
    const objectStore = transaction.objectStore("om3_flag");

    return new Promise((resolve, reject) => {
        if (indexDb === null) {
            resolve('')
        }
        const getRequest = objectStore.get(url);

        getRequest.onsuccess = function (event: any) {
            const result = event.target.result;
            if (result) {
                const byteArray = result.binaryData;
                //@ts-ignore
                const byteCharacters = Array.from(byteArray, byte => String.fromCharCode(byte)).join('');
                const base64String = btoa(byteCharacters);
                // 使用转换后的 Base64 数据
                resolve(base64String)
            } else {
                // 数据不存在
                resolve('')
            }
        };

        getRequest.onerror = function (event: any) {
            reject("get flag fail")
            // 读取失败
        };
    })
}

export function clearFlagDB() {
    if (isOpen && indexDb) {
        indexDb.close();
        indexedDB.deleteDatabase("om_flag_db");
        isOpen=false
        console.warn("clear indexdb flag");
    }

}