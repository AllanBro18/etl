require('dotenv').config();
let mysql = require('mysql2/promise');

const config = {
  host: process.env.DB_HOST,
  user: process.env.DB_USERNAME,
    password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME
};

async function runETL() {
    let connection;
    try {
        connection = await mysql.createConnection(config);
        console.log('Connected');

        await etlMenuDimension(connection);
        await etlCustomerDimension(connection);
        await etlFactSales(connection);

        console.log('Success');

    } catch (error) {
        console.error('Error:', error.message);
    } finally {
        if (connection) {
            await connection.end();
            console.log('Database Connection Terminated');
        }
    }
}


async function etlMenuDimension(connection) {
    const EXTRACT_MENU_QUERY = `
        SELECT menu_id, nama_item, deskripsi, harga FROM Menu
    `;
    const [oltpMenu] = await connection.execute(EXTRACT_MENU_QUERY);
    
    if (oltpMenu.length === 0) {
        console.log('Data empty');
        return;
    }
    
    const menuRecords = oltpMenu.map(item => [
        item.menu_id, 
        item.nama_item, 
        item.deskripsi, 
        (item.deskripsi && item.deskripsi.toLowerCase().includes('minum')) ? 'Minuman' : 'Makanan',
        item.harga
    ]);

    const LOAD_MENU_QUERY = `
        INSERT INTO Dim_Menu 
        (menu_id, nama_item, deskripsi_item, kategori_item, harga_saat_ini)
        VALUES ?
        ON DUPLICATE KEY UPDATE
            nama_item = VALUES(nama_item),
            deskripsi_item = VALUES(deskripsi_item),
            kategori_item = VALUES(kategori_item),
            harga_saat_ini = VALUES(harga_saat_ini)
    `;
    
    await connection.query(LOAD_MENU_QUERY, [menuRecords]);
    
    console.log('Dim_Menu updated');
}

async function etlCustomerDimension(connection) {    
    const EXTRACT_CUST_QUERY = `
        SELECT pelanggan_id, nama_pelanggan, nomor_telepon, alamat FROM Pelanggan
    `;
    const [oltpCustomers] = await connection.execute(EXTRACT_CUST_QUERY);
    
    const customerRecords = oltpCustomers.map(cust => [
        cust.pelanggan_id, 
        cust.nama_pelanggan, 
        cust.nomor_telepon,
        cust.alamat
    ]);

    const LOAD_CUST_QUERY = `
        INSERT INTO Dim_Pelanggan 
        (pelanggan_id, nama_pelanggan, nomor_telepon, alamat)
        VALUES ?
        ON DUPLICATE KEY UPDATE
            nama_pelanggan = VALUES(nama_pelanggan),
            nomor_telepon = VALUES(nomor_telepon),
            alamat = VALUES(alamat)
    `;
    
    await connection.query(LOAD_CUST_QUERY, [customerRecords]);
    
    console.log('Dim_Pelanggan updated');
}


async function etlFactSales(connection) {
    const EXTRACT_FACT_QUERY = `
        SELECT
            dp.pesanan_id,
            dp.menu_id,
            dp.kuantitas,
            dp.subtotal AS pendapatan_item,
            p.tanggal_waktu_pesanan
        FROM
            Detail_Pesanan dp
        JOIN
            Pesanan p ON dp.pesanan_id = p.pesanan_id
        WHERE
            p.status_pesanan = 'Selesai'`;

    const [factData] = await connection.execute(EXTRACT_FACT_QUERY);
    
    if (factData.length === 0) {
        console.log('No new data');
        return;
    }

    const factRecords = [];
    for (const item of factData) {
        const orderTime = new Date(item.tanggal_waktu_pesanan);
        const dateKey = orderTime.getFullYear() * 10000 + (orderTime.getMonth() + 1) * 100 + orderTime.getDate();
        const menuKey = item.menu_id; 

        factRecords.push([
            dateKey,
            menuKey,
            item.pesanan_id,
            item.kuantitas,
            item.pendapatan_item 
        ]);
    }

    const LOAD_FACT_QUERY = `
        INSERT INTO Fact_Penjualan_Item 
        (date_key, menu_key, pesanan_id, kuantitas_terjual, pendapatan_item)
        VALUES ?
    `;
    
    await connection.query(LOAD_FACT_QUERY, [factRecords]);

    console.log("Fact_item_sales updated");
}

runETL();