const mysql = require("mysql2");
const dotenv = require("dotenv");
const admin = require("firebase-admin");

dotenv.config();
let conection = null;

function dbConection() {
  try {
    conection = mysql.createConnection({
      host: process.env.DB_HOST?.toString(),
      port: Number(process.env.DB_PORT?.toString()),
      user: process.env.DB_USER?.toString(),
      password: process.env.DB_PASS?.toString(),
      database: process.env.DB_NAME?.toString(),
    });
  } catch (error) {
    console.log("connection error: ", error);
  }
}
async function executeQuery(query) {
  try {
    const result = await conection.promise().query(query);
    return result;
  } catch (error) {
    console.log("error in execute query", error);
  }
}
async function rollback() {
  try {
    const result = await conection.promise().query("rollback");
    return result;
  } catch (error) {
    console.log("error in rollback", error);
  }
}
async function insertUser(users) {
  const errorsControl = [];
  try {
    for (let i = 0; i < users.length; i++) {
      try {
        await dbConection();
      } catch (error) {
        console.log(error);
        return {
          statusCode: 500,
          body: JSON.stringify("Error in the database connection"),
        };
      }
      const user = users[i];

      const queryUser = `insert into user (email, user_name, phone, position) values ('${user.email}', '${user.user_name}', '${user.phone}', '${user.position}')`;
      const result = await executeQuery(queryUser);

      if (result.affectedRows == 0) {
        errorsControl.push(user.email);
      }
      users[i].user_id = result[0].insertId;
    }
    console.log("users inserted");
    return errorsControl;
  } catch (error) {
    await rollback();
    console.log("error", error);
  }
}
async function insertCompanyAssigment(users, project_id) {
  const errorsControl = [];
  try {
    for (let i = 0; i < users.length; i++) {
      const userTemp = users[i];
      try {
        await dbConection();
      } catch (error) {
        console.log(error);
        return {
          statusCode: 500,
          body: JSON.stringify("Error in the database connection"),
        };
      }
      let queryCompany = `insert into company_assigments (rol_id, user_id, project_id) values ('${userTemp.rol_id}', '${userTemp.user_id}', '${project_id}')`;
      const result = await executeQuery(queryCompany);
      if (result.affectedRows == 0) {
        errorsControl.push(user.email);
      }
    }
    console.log("company assigments inserted");
    return errorsControl;
  } catch (error) {
    await rollback();
    console.log("error", error);
  }
}
async function firebaseConection() {
  try {
    const serviceAccount = {
      projectId: process.env.FIREBASE_PROJECT_ID?.toString(),
      privateKey: process.env.FIREBASE_PRIVATE_KEY?.replace(
        /\\n/g,
        "\n"
      )?.toString(),
      clientEmail: process.env.FIREBASE_CLIENT_EMAIL?.toString(),
    };
    if (admin.apps.length > 0) {
      return;
    }
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
    });
  } catch (error) {
    console.log("error", error);
  }
}

async function dropConectionFirebase() {
  try {
    await admin.app().delete();
  } catch (error) {
    console.log("error", error);
  }
}
async function insertUserFirebase(users) {
  const errorsControl = [];
  try {
    for (let i = 0; i < users.length; i++) {
      const email = users[i].email;
      const password = users[i].password;
      await admin
        .auth()
        .getUserByEmail(email)
        .catch((err) => {
          return null;
        });
      const result = await admin.auth().createUser({
        email,
        password,
      });
      if (result == null) {
        errorsControl.push(email);
      }
    }
    console.log("users firebase inserted");
    return errorsControl;
  } catch (err) {
    console.log(err);
    return null;
  }
}

async function insertBussinesRules(bussines, users) {
  try {
    for (let i = 0; i < bussines.length; i++) {
      const bussinesTemp = bussines[i];
      console.log(bussinesTemp);
      await dbConection();
      for (let i = 0; i < bussinesTemp.user.length; i++) {
        const user_id = await findUserIdByEmail(bussinesTemp.user[i], users);
        
        for (let i = 0; i < bussinesTemp.channel.length; i++) {
          try {
            const query = `insert into user_channel (user_id, channel_id) values ('${user_id}', '${bussinesTemp.channel[i]}')`;
            const result = await executeQuery(query);
           
          } catch (error) {
            console.log("error", error);
            throw new Error("error in channel insertion");
          }
        }
        for (let i = 0; i < bussinesTemp.line.length; i++) {
          try {
            const query = `insert into user_line (user_id, line_id) values ('${user_id}', '${bussinesTemp.line[i]}')`;
            const result = await executeQuery(query);
          
          } catch (error) {
            console.log("error", error);
            throw new Error("error in line insertion");
          }
        }
        for (let i = 0; i < bussinesTemp.subline.length; i++) {
          try {
            const query = `insert into user_subline (user_id, subline_id) values ('${user_id}', '${bussinesTemp.subline[i]}')`;
            const result = await executeQuery(query);
         
          } catch (error) {
            console.log("error", error);
            throw new Error("error in subline insertion");
          }
        }
        for (let i = 0; i < bussinesTemp.zone.length; i++) {
          try {
            const query = `insert into user_zone (user_id, zone_id) values ('${user_id}', '${bussinesTemp.zone[i]}')`;
            const result = await executeQuery(query);
            
          } catch (error) {
            console.log("error", error);
            throw new Error("error in zone insertion");
          }
        }
      }
    }
    console.log("bussines rules inserted");
  } catch (error) {
    console.log("error in bussines insertion", error);
  }
}

async function findUserIdByEmail(email, users) {
  const user = users.find((user) => user.email == email);
  return user.user_id;
}

async function dropUserFirebase(users) {
  try {
    for (let i = 0; i < users.length; i++) {
      const email = users[i].email;
      await admin
        .auth()
        .getUserByEmail(email)
        .then((user) => {
          admin.auth().deleteUser(user.uid);
        });
    }
  } catch (error) {
    console.log("error", error);
  }
}

exports.handler = async function (event) {
  const users = event.users;
  const project_id = event.project_id;
  const bussines = event.bussines_rules;
  try {
    await firebaseConection();
  } catch (error) {
    console.log(error);

    return {
      statusCode: 500,
      body: JSON.stringify("error in firebase connection"),
    };
  }
  const userFirebaseRes = await insertUserFirebase(users);
  if (userFirebaseRes == null) {
    console.log("error in firebase insert");
    await dropUserFirebase();
    return {
      statusCode: 500,
      body: JSON.stringify("error in firebase insert"),
    };
  }
  const userRes = await insertUser(users);
  const companyRes = await insertCompanyAssigment(users, project_id);
  await insertBussinesRules(bussines, users);

  if (userRes.length > 0) {
    console.log("error in user insert");
    return {
      statusCode: 500,
      body: JSON.stringify("error in user insert"),
    };
  }

  if (companyRes.length > 0) {
    console.log("error in company insert");
    return {
      statusCode: 500,
      body: JSON.stringify("error in company insert"),
    };
  }

  await dropConectionFirebase();
};
