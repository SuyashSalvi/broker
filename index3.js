var http = require('http');
var mysql = require('mysql2');
var mysql2 = require('mysql2/promise');
var express = require('express');
const bodyParser = require('body-parser');
require('dotenv').config()

//variables
var publicIpAddress = "";
var leaderNode = "";
var gossipNeighbor;
var neighbors = [];
var queues = {};
var conn; //connection to mysql2/promise
var serverQueue = {};

//init express
var app = express();
app.use(bodyParser.json());
app.set('port', process.env.PORT || 3000);

//#region update the server name in db 
// Function to get the public IP address of the server
const getPublicIpAddress = () => {
    return new Promise((resolve, reject) => {
      const options = {
        hostname: 'httpbin.org',
        port: 80,
        path: '/ip',
        method: 'GET',
      };
  
      const req = http.request(options, (res) => {
        let data = '';
  
        res.on('data', (chunk) => {
          data += chunk;
        });
  
        res.on('end', () => {
          const responseData = JSON.parse(data);
          publicIpAddress = responseData.origin + ":" + app.get('port');
          console.log(`server is Running on ${publicIpAddress}`);
          resolve(publicIpAddress);
        });
      });
  
      req.on('error', (error) => {
        reject(error);
      });
  
      req.end();
    });
};

// Function to update the public IP address in the MySQL database
const updatePublicIpAddressInDatabase = async () => {
    try {
        const publicIpAddress = await getPublicIpAddress();
        const dbHost = process.env.DB_HOST;
        const dbUser = process.env.DB_USER;
        const dbPassword = process.env.DB_PASSWORD;
        const dbname = process.env.DB_NAME;

        const db = {
            host: dbHost,
            user: dbUser,
            password: dbPassword,
            database: dbname,
        };

        conn = await mysql2.createConnection(db);
        //get leader node
        let [rows, fields] = await conn.execute('Select name from servers where isLeader = 1');
        // console.log(rows[0][fields[0]['name']]);
        var recordId = 1;
        if (rows.length !=0){
            recordId = 0;
            leaderNode = rows[0][fields[0]['name']]
        }
        else{
            leaderNode = publicIpAddress; 
        }

        // insert the new node in db and update it to leader if no leader exist
        [rows,fields] = await conn.execute('INSERT into servers (name,isLeader) values (?,?)',[publicIpAddress,recordId]);
        console.log(`Node ${publicIpAddress} is online.!`);
        console.log(`Leader Node is -> ${leaderNode}`);

        //balance queues(call to leader node)
        sendBalanceNodeRequest();
    } catch (error) {
        console.error(`Error retrieving public IP address: ${error.message}`);
    }
};

function sendBalanceNodeRequest(){
    try{
        var server = leaderNode.trim().split(":");
        var options = {
            hostname: server[0],
            // hostname: 'localhost',
            port: server[1],
            path: '/balanceNodes',
            method: 'GET',
        };
        var req1 = http.request(options,(res1) =>{
            res1.on('end', () => {
                if (res.statusCode != 200) {
                    console.error('Request failed with status:', res.statusCode);
                }
            });
        });
        req1.on('error', (error) => {
            console.log(error);
        });
        req1.end();
    }
    catch(error){
        console.log(error);
    }
}

//#endregion

//#region gossip
function gossip(){
    gossipNeighbor = setInterval(() => {
        // console.log("neighbors for heartbeat: ", neighbors);
        for(var i=0; i < neighbors.length; ++i){
            const server = neighbors[i].trim().split(":");
            const options = {
                hostname: server[0],
                // hostname: 'localhost',
                port: server[1],
                path: '/healthcheck',
                method: 'GET',
              };
            const req1 = http.request(options);
            req1.on('error', (error) => {
                console.log(error);
                clearInterval(gossipNeighbor);
                console.log('neighbor down: ', server[0]+":"+server[1]);
                neighbourDown(server[0]+":"+server[1]);
              });
            req1.end();
        }
    },2000);
}

async function neighbourDown(node){
    //check if the neighbor is leader
    //delete neighbor
    //elect leader if not
    //send balanceNode request to leader
    // await conn.beginTransaction();
    try{
        var [val,] = await conn.execute('select name from servers where name = ?',[node]);
        if(val.length == 0)
            return;
        await conn.execute('delete from servers where name = ?',[node]);
        [val,] = await conn.execute('select name from servers where isLeader = 1');
        if(val.length == 0){
            [val,] = await conn.execute('select name from servers order by id desc limit 1');
            await conn.execute('update servers set isLeader = 1 where name = ?',[val[0].name]);
            leaderNode = val[0].name;
        }
        // await conn.commit();
        leaderNode = val[0].name;
        console.log("leader node update. new leader :", leaderNode);
        const server = leaderNode.split(":");
        var options = {
            hostname: server[0],
            // hostname: 'localhost',
            port: server[1],
            path: '/balanceNodes',
            method: 'GET',
        };
        var req1 = http.request(options);
        req1.on('error', (error) => {
            console.log(error);
        });
        req1.end();
    }
    catch(error){
        // await conn.rollback();
        console.error('Error during transaction:', error);
    }
    
}
//#endregion

//#region ratelimiter
// Rate limiting parameters
const maxRequests = 5; // Maximum number of requests allowed in the time frame
const timeFrame = 2 * 1000; // Time frame in milliseconds (e.g., 1 minute)

let requestCount = 0;
let lastResetTime = Date.now();

// Middleware to check and handle rate limiting
const rateLimiter = (req, res, next) => {
  const currentTime = Date.now();

  // Check if the time frame has passed since the last reset
  if (currentTime - lastResetTime > timeFrame) {
    // If the time frame has passed, reset the counter
    requestCount = 0;
    lastResetTime = currentTime;
  }

  // Check if the maximum number of requests has been reached
  if (requestCount < maxRequests) {
    // If within limits, increment the counter and proceed with the request
    requestCount++;
    next();
  } else {
    // If the limit is exceeded, send a 429 (Too Many Requests) response
    res.status(429).json({ error: 'Rate limit exceeded. Try again later.' });
  }
};
//#endregion


function getMessages(options, postData) {
    return new Promise((resolve, reject) => {
        const jsonData = JSON.stringify(postData);
        const req = http.request(options, (res) => {
            let data = '';
            res.on('data', (chunk) => {
            data += chunk;
            }); 
            res.on('end', () => {
            resolve(JSON.parse(data));
            });
        });
        req.write(jsonData);
        req.on('error', (error) => {
            reject(error);
        });
        req.end();
    });
  }

//#region api start
app.get('/', (req, res) => {
    res.send('Hello, this is a broker app written on Node.js!');
});

app.get('/healthcheck', async (req, res) => {
    res.status(200).send("OK");
});

app.get('/balanceNodes', rateLimiter, async (req, res) => {
    console.log("balacing nodes");
    let [servers,] = await conn.execute('Select name from servers');
    let [queueMaster,] = await conn.execute('select name from queue_master');

    var queueServer = {};
    var j = 0;
    for(var i=0; i < queueMaster.length; i++){
        if(j==servers.length)
            j = 0;
        serverQueue[queueMaster[i]['name']] = servers[j]['name'];
        if(queueServer[servers[j]['name']]){
            queueServer[servers[j]['name']] += "," + queueMaster[i]['name'];
        }
        else{
            queueServer[servers[j]['name']] = queueMaster[i]['name'];
        }
        j++;
    }
    neighbors = {};
    console.log("servers that are up and running are as follow:");
    console.log(servers);
    if(servers.length == 1){
        neighbors[servers[0]['name']] = null;
    }
    else if(servers.length == 2){
        neighbors[servers[0]['name']] = servers[1]['name'];
        neighbors[servers[1]['name']] = servers[0]['name'];
    }
    else{
        servers.push(servers[0]);
        servers.push(servers[1]);
        for(var i=0; i < servers.length-2; i++){
            neighbors[servers[i]['name']] = servers[i+1]['name'] + "," + servers[i+2]['name'];
        }
        servers.splice(-2);
    }
    insertTable = [];
    for(var i=0; i < servers.length; i++){
        insertTable.push([servers[i].name,"",""]);
        if(neighbors[servers[i].name]){
            insertTable[i][2] = neighbors[servers[i].name];
        }
        if(queueServer[servers[i].name]){
            insertTable[i][1] = queueServer[servers[i].name];
        }
    }
    console.log(" arrary of server_name,queues,gossip_neighbors");
    console.log(insertTable);
    var values = insertTable.flat(Infinity); 
    try{
        await conn.execute('Delete from server_to_queue_map');
        await conn.execute('Insert into server_to_queue_map (server_name,queues,neighbors) values '+servers.map((_, index) => '(?, ?, ?)').join(', '),values);
    }
    catch(error){
        console.log(error);
    }
    console.log("sending the leader node to the server hosting the website");
    server = "54.153.52.213:8080".trim().split(":");
    const options1 = {
        hostname: server[0],
        // hostname: 'localhost',
        port: server[1],
        path: '/api/v1/publisher/fetchBrokerIp',
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
    };
    const req2 = http.request(options1);
    req2.write(JSON.stringify({"ipAddress": publicIpAddress}));
    req2.on('error', (error) => {
        console.error('Error:', error.message);
    });
    req2.end();

    console.log("nodes are balanced as per queues. braodcasting reqeust to nodes to get their queues");
    res.status(200).send(insertTable);

    for(var i=0; i < servers.length; i++){
        var server = servers[i].name.split(":");
        var options = {
            hostname: server[0],
            // hostname: 'localhost',
            port: server[1],
            path: '/balanceQueues',
            method: 'GET',
          };
        var req1 = http.request(options);
        req1.on('error', (error) => {
            console.log(error);
          });
        req1.end();
    }
});

app.get('/balanceQueues', async (req, res) => {
    clearInterval(gossipNeighbor);
    const [data,] = await conn.execute(`Select queues,neighbors from server_to_queue_map where server_name = '${publicIpAddress}'`);
    console.log(`queues to manage and neighbors to gossip about heartbeat`);
    console.log(data);
    const q = data[0].queues.trim().split(",");
    neighbors = data[0].neighbors ? data[0].neighbors.trim().split(","): [];
    var [newQueueData,] = await conn.execute(`select queue_name,message from queue where queue_name in (`+ q.map((_, index) => '?').join(', ')+`)`, q);
    queues = {};
    for(var i=0; i < newQueueData.length; i++){
        if(queues[newQueueData[i].queue_name]){
            queues[newQueueData[i].queue_name].push(newQueueData[i].message);
        }
        else{
            queues[newQueueData[i].queue_name] = [newQueueData[i].message];
        }
    }
    for(var i=0; i < q.length; i++){
        if(!queues[q[i]]){
            queues[q[i]] = [];
        }
    }
    res.status(200).send(queues);
    gossip();
});

app.post('/publishData', (req, res) => {
    try{
        const postData = req.body;
        console.log(postData);
        const server = serverQueue[postData.publishSector].trim().split(":");
        var options = {
            hostname: server[0],
            // hostname: 'localhost',
            port: server[1],
            path: '/publish-message',
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
              },
        };
        var req1 = http.request(options,(res1)=>{
            res.status(200).send("OK");
        });
        req1.on('error', (error) => {
            console.log(error);
        });
        req1.write(JSON.stringify(postData));
        req1.end();
    }
    catch(error){
        console.log(error);
    }
});

app.post('/publish-message', async (req, res) => {
    const postData = req.body;
    const message = postData.publishMessage;
    const q = postData.publishSector;
    if(queues[q]){
        queues[q].push(message);
    }
    else{
        queues[q] = [message];
    }
    await conn.execute('Insert into queue (message,queue_name) values (?,?)',[message,q]);
    // console.log(`message : ${message} added to the queue: ${q}`);
    res.status(200).send("OK");
});

app.post('/fetchPublishedData', (req, res) => {
    const postData = req.body;
    var requests = [];
    try{
        for(var i=0; i  < postData.length; i++){
            const server = serverQueue[postData[i].publishSector].trim().split(":");
            var options = {
                hostname: server[0],
                // hostname: 'localhost',
                port: server[1],
                path: '/get-message',
                method: 'POST',
                headers: {
                  'Content-Type': 'application/json',
                },
            };
            // console.log("requeust forwarded to : ", server[0]);
            requests.push(getMessages(options,postData[i]));
        }
        Promise.all(requests)
            .then((responses) => {
                // console.log("final response for the reqeuest", responses);
                res.status(200).send(responses);
            })
            .catch((error) => {
                console.error('Error:', error.message);
            });
    }
    catch(error){
        console.log(error);
    }
});

app.post('/get-message', (req, res) => {
    const postData = req.body;
    // console.log("message body",req.body);
    const output = {publishSector: postData.publishSector, messages: queues[postData.publishSector].slice(postData.offset ? postData.offset : 0).reverse()};
    // console.log("message output",output);
    res.status(200).send(output);
});

app.get("/recursion",(req,res)=>{
    const options = {
        hostname: 'localhost',
        port: 3000,
        path: '/',
        method: 'GET',
      };
  
    const req1 = http.request(options, (res1) => {
        let data = '';
        res1.on('data', (chunk) => {
            data += chunk;
        });

        res1.on('end', () => {
            // const responseData = JSON.parse(data);
            res.send(data);
        });
    });
    req1.on('error', (error) => {
        reject(error);
      });
    req1.end();
});

app.get('/getLeader', (req, res) => {
    console.log("get leader - starts -");
    res.send('Hello, this is a basic Node.js API!');
});
//#endregion

updatePublicIpAddressInDatabase();
//getleader
//get 2 nodes for gossip
//if gossip fails then send to leader to reupdate the nodes
//leader sends gossip to rexamine their queues
//gossip starts again

app.listen(app.get('port'), function() {
    console.log('Message Broker server listening on port %d', app.get('port'));
});