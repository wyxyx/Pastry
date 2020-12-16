open Akka.Actor
open Akka.FSharp
open System.Threading
open System

//functions
let toHex(num : int) = 
    let mutable res = ""
    if num = 0 then
        res <- "0"
    else
        let mutable num1 = num
        let mutable hexArray:string list = ["0";"1";"2";"3";"4";"5";"6";"7";"8";"9";"A";"B";"C";"D";"E";"F"] 
        let mutable resSize = 0
        let numberf = 0xF
        while num1 <> 0 && resSize < 8 do
            res <- hexArray.[num1 &&& numberf] + res
            num1 <- num1 >>> 4
            resSize <- resSize + 1
    res

let HextoDecimal(num : string) = 
    let num1 = num.ToUpper()
    let len = num.Length
    let mutable numbase = 1
    let mutable temp = 0
    for i = len-1 downto 0 do
        if num1.[i]>='0' && num1.[i]<='9' then
            temp <- temp + (int num1.[i] - 48) * numbase
            numbase <- numbase * 16
        elif num1.[i]>='A' && num1.[i]<='F' then
            temp <- temp + (int num1.[i] - 55) * numbase
            numbase <- numbase * 16
    temp

let getKeys(map : Map<'K,'V>) =
    seq {
        for KeyValue(key,value) in map do
            yield key
    } |> Set.ofSeq

let toList s = Set.fold (fun l se -> se::l) [] s

let rec reverseList list =
    match list with
    |[] -> []
    |[x] -> [x]
    | head::tail -> reverseList tail @ [head]

let updateElement index element list = 
    list |> List.mapi (fun i v -> if i = index then element else v)

type Data = 
    | Initialize of nodeId:string * numDigits:int
    | Route of key:string * source:string * hop:int
    | Join of nodeId:string * currentIndex:int
    | UpdateRoutingTable of routingTable:string list

let mutable map = Map.empty
let mutable hopmap = Map.empty<string,int list>

//actor
let workerActor (mailbox: Actor<_>) = 
    let mutable nodeId = ""
    let mutable rows = 0
    let mutable cols = 16
    let mutable routingTable  = Array2D.zeroCreate rows cols
    let mutable leafSet = Set.empty
    let mutable lenCommonPrefix = 0
    let mutable currentRow = 0
    let rec loop () = actor {
        let! message = mailbox.Receive ()
        match message with
        | Initialize(i,d)->
            nodeId <- i
            rows <- d
            routingTable <- Array2D.zeroCreate rows cols
            let mutable iteration = 0
            //from hex to int
            let number = HextoDecimal i
            let mutable left = number
            let mutable right = number
            while iteration < 8 do
                if left = 0 then
                    left <- map.Count - 1
                let mutable leftHex = toHex left
                leafSet <- leafSet.Add(leftHex)
                iteration <- iteration + 1
                left <- left - 1
            while iteration < 16 do
                if right = map.Count - 1 then
                    right <- 0
                let mutable rightHex = toHex right
                leafSet <- leafSet.Add(rightHex)
                iteration <- iteration + 1
                right <- right + 1
        | Join(key,currIndex)->
            let mutable i = 0
            let mutable k = currIndex
            while key.[i] = nodeId.[i] do
                i <- i + 1
            lenCommonPrefix <- i
            let mutable routingRow : string list =[]
            while k <= lenCommonPrefix do
                let len2 = Array2D.length2 routingTable
                for num =len2-1 downto 0  do
                    routingRow <- routingTable.[k,num] :: routingRow
                let rowindex = HextoDecimal (nodeId.[lenCommonPrefix].ToString())
                routingRow <- updateElement rowindex nodeId routingRow
                //routingRow.[rowindex] = nodeId
                map.[key] <! UpdateRoutingTable(routingRow)
                k <- k + 1
            let rtRow = lenCommonPrefix
            let rtCol = HextoDecimal (key.[lenCommonPrefix].ToString())

            if routingTable.[rtRow,rtCol] = null then//insert a new key to routing table
                routingTable.[rtRow,rtCol] <- key
            else//update routing table
                map.[routingTable.[rtRow,rtCol]] <! Join(key,k)
        | UpdateRoutingTable(r:string list) ->
            //len2: length of column
            let len2 = Array2D.length2 routingTable
            for num = 0 to len2-1 do
                routingTable.[currentRow,num] <- r.[num]
            currentRow <- currentRow + 1
        | Route(key,source,hops) ->
            if key = nodeId then
                if hopmap.ContainsKey(source) then
                    let total =  hopmap.[source].[1]
                    let avgHops = hopmap.[source].[0]
                    //update element in hopmap
                    hopmap <- hopmap.Remove(source)//Remove 
                    let slist = [((avgHops*total)+hops)/(total+1);total+1]
                    hopmap <- hopmap.Add(source,slist)
                else
                    let mutable h:int list = [hops;1];
                    hopmap <- hopmap.Add(source,h)
            elif leafSet.Contains(key) then// use the leafset
                map.[key] <! Route(key,source,hops+1)
            else//use the routing table
                let mutable i = 0
                while key.[i] = nodeId.[i] do
                    i <- i + 1
                lenCommonPrefix <- i
                let mutable rtRow = lenCommonPrefix
                let mutable rtCol = HextoDecimal (key.[lenCommonPrefix].ToString())
                if routingTable.[rtRow,rtCol] = null then
                    rtCol <- 0
                map.[routingTable.[rtRow,rtCol] ] <! Route(key,source,hops+1)
        return! loop ()
    }
    loop ()
//main
[<EntryPoint>]
let main argv =
    printfn "Please input values as format: \"project3 numNodes numRequest\""
    let line = System.Console.ReadLine()
    let arg = line.Split ' '
    let proj3 = string arg.[0]
    let numNodes = int arg.[1]
    let numRequests = int arg.[2]

    if proj3 = "project3" && numNodes > 0 && numRequests > 0 then
        let numDigits = int (ceil (Math.Log(float(numNodes), 16.)))
        let mutable nodeId = ""
        let mutable numOfHex = ""
        let mutable len = 0
        let mutable strZero = ""
        let system = ActorSystem.Create "MySystem"
        let mutable worker = null
        worker <- spawn system "worker0" workerActor
        for i=0 to numDigits-1 do
            nodeId <- nodeId + "0"
        worker <! Initialize(nodeId,numDigits)
        map <- map.Add(nodeId, worker)
        for i=1 to numNodes-1 do
            if i = 1 then
                printfn "Building Pastry system..." 
            numOfHex <- toHex i
            len <- numOfHex.Length
            nodeId <- ""
            for j=0 to numDigits-len-1 do
                nodeId <- nodeId + "0"
            nodeId <- nodeId + numOfHex
    
            worker <- spawn system ("worker"+i.ToString()) workerActor
            worker <! Initialize(nodeId,numDigits)
            map <- map.Add(nodeId, worker)

            strZero <- ""
            for j=0 to numDigits-1 do
                strZero <- strZero + "0"
            map.[strZero] <! Join(nodeId,0)
            Thread.Sleep(5)
        Thread.Sleep(1000)
        printfn "Pastry system is built"
        printfn "Processing Requests..."
        //use functions
        let setKeys = getKeys map
        let mutable arrayKeys  = toList setKeys
        let mutable k = 1
        let mutable desId = ""
        let mutable ctr = 0
        while k<= numRequests do
            for id in arrayKeys do
                ctr <- ctr + 1
                desId <- id
                while desId = id do
                    desId <- arrayKeys.[System.Random().Next() % numNodes]
                map.[id] <! Route(desId,id,0)
                Thread.Sleep(5)
            printfn "Each node has performed %A requests" k
            k <- k + 1
        Thread.Sleep(1000)
        printfn "All requests have processed"
        let mutable sizeHop = 0.0
        let mutable numOfHop = 0.0
        for m in hopmap do
            numOfHop <- numOfHop + 1.0
            sizeHop <- sizeHop + float m.Value.[0]
        let avg = sizeHop/numOfHop
        printfn "The average hop size is %A " avg
    else 
        printfn "invalid input."
    0 // return an integer exit code