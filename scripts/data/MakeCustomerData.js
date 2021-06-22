
var fs = require('fs');

var headers = "idt,PhoneNumber1,PhoneNumber2,AccountNumber,AccountStatus,HouseNumber,HouseNumberSuffix,StreetName,StreetSuffix,City,State,PostCode,AccountName,EmailAddress,DateOfBirth,RateClass,CollectionClassification,ServiceType,InventoryType,OutageInformation,OpenConnect,OpenServiceAdd,OpenDisconnect,OpenServiceCall,OpenServiceTypeStatus,AppointmentStartDateTime,AppointmentEndDateTime,OpenServiceMove,OpenIQUpgrade,SuspendStatus,CollectionSegment,TelstraBundleFlag,Suspension_Reason,DiyCount,HybridHome,Tv,Voice,Broadband,FoxtelPlay,VoiceTv_ServiceOrderId,TechType,PaymentMethod,CaseMgmt,WFIFlag,Enrolment_Status,Priority_Status,Loyalty_Band,Billing_Repeat\n";

var streetTypes = [
    'STREET',
    'STREET',
    'STREET',
    'STREET',
    'STREET',
    'ROAD',
    'ROAD',
    'ROAD',
    'AVENUE',
    'AVENUE',
    'LANE'
];

var stateData =
[
    {
        state: 'QLD',
        min: 4000,
        max: 4700
    },
    {
        state: 'NSW',
        min: 2000,
        max: 2600
    },
    {
        state: 'VIC',
        min: 3000,
        max: 3800
    },
    {
        state: 'ACT',
        min: 2600,
        max: 2800
    },
    {
        state: 'SA',
        min: 5000,
        max: 5400
    },
    {
        state: 'WA',
        min: 6000,
        max: 6400
    },
    {
        state: 'TAS',
        min: 7000,
        max: 7300
    }
];

function makeCustomerData(rows, outputFile)
{

    var writer = fs.createWriteStream(outputFile);

    writer.write(headers);

    var sequenceNumber = 100;
    var accountNumber = 100000;

    var femaleNames = fs.readFileSync('data/female_names.txt', 'UTF-8').split('\n');
    var maleNames = fs.readFileSync('data/male_names.txt', 'UTF-8').split('\n');
    var lastNames = fs.readFileSync('data/last_names.txt', 'UTF-8').split('\n');

    var streetNames = fs.readFileSync('data/street_names.txt', 'UTF-8').split('\n');
    var suburbNames = fs.readFileSync('data/suburb_names.txt', 'UTF-8').split('\n');

    for (var i = 0; i < rows; i++)
    {
        var phoneNumber1 = 400000000 + Math.floor(Math.random() * 99999999);
        var phoneNumber2 = 400000000 + Math.floor(Math.random() * 99999999);

        var firstName = null;

        if (Math.random() > 0.5)
        {
            firstName = femaleNames[Math.floor(Math.random() * femaleNames.length)];
        }
        else
        {
            firstName = maleNames[Math.floor(Math.random() * maleNames.length)];
        }

        var lastName = lastNames[Math.floor(Math.random() * lastNames.length)];
        var name = firstName + ' ' + lastName;

        var streetNumber = 1 + Math.floor(Math.random() * 350);
        var streetName = streetNames[Math.floor(Math.random() * streetNames.length)];
        var streetType = streetTypes[Math.floor(Math.random() * streetTypes.length)];
        var suburb = suburbNames[Math.floor(Math.random() * suburbNames.length)];

        var statePostCode = getStatePostCode();

        var dob = getDateOfBirth();

        writer.write(`${sequenceNumber},0${phoneNumber1},0${phoneNumber2},${accountNumber},-1,${streetNumber},,${streetName},${streetType},${suburb},${statePostCode.state},${statePostCode.postcode},${firstName} ${lastName},test@amazon.com,${dob},1000,99,8,502,,0,1,0,0,10,,,0,0,0,2,,,,0,0,0,0,0,,0,1,0,0,0,0,,0\n`);

        sequenceNumber++;
        accountNumber++;
    }

    writer.on('finish', () => {
     console.error('All writes are now complete.');
    });

    writer.close();

    console.log('Complete');
}

function getStatePostCode()
{
    var state = stateData[Math.floor(Math.random() * stateData.length)];
    var postcode = state.min + Math.floor(Math.random() * (state.max - state.min));

    return {
        state: state.state,
        postcode: postcode
    };
}

function getDateOfBirth()
{

    var day = 1 + Math.floor(Math.random() * 28);

    if (day < 10)
    {
        day = '0' + day;
    }

    var month = 1 + Math.floor(Math.random() * 12);

    if (month < 10)
    {
        month = '0' + month;
    }

    var year = 1950 + Math.floor(Math.random() * (2003 - 1950));

    return `${day}/${month}/${year}`;
}

makeCustomerData(100, './data/customer-001.csv');