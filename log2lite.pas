(*
  Simple little test to see how quickly FPC reads an Apache log file, performs a regexp function
  and inserts the result into an SQLite3 database

  Uses BRRE https://github.com/BeRo1985/brre

  -- Some results --

  test1 method:
    224639 records
    runtime: 00h 02m 39s
  test2 method:
    224639 records
    runtime: 00h 02m 29s
  test2 method with -O3 -Xs switches:
    224639 records
    runtime: 00h 02m 07s
    {$mode delphi} reduces runtime by another second
    
    Losing the ExtractStats function .... runtime 27 seconds!
    
  test1 without ExtractStats function: 29 seconds    
 *)
program log2lite;

{$mode delphi}

uses cmem, Classes, SysUtils, DateUtils, sqldb, db, sqlite3ds, sqlite3conn, BRRE, BRREUnicode;

type
  PStatsLine = ^TStatsLine;
  TStatsLine = Packed Record
    Ip: String;
    DateTime: String;
    Method: String;
    ReqFile: String;
    Code: Integer;
    FileSize: LongInt;
    Referrer: String;
    UserAgent: String;
  end;  

var
  startTime: TDateTime;
  endTime: TDateTime;

const  
  Expression = '/^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] \"(\S+) (.*?) (\S+)\" (\S+) (\S+) "([^"]*)" "([^"]*)"$/';    

(*function ExtractStats(lineIn: String): TStatsLine;
var
  RegExp: TBRRERegExp;
  MatchAll: TBRRERegExpStrings;
begin
  RegExp := TBRRERegExp.Create(Expression);
  MatchAll := RegExp.Split(lineIn);
  if Length(MatchAll) = 13 then
  begin
    Result.Ip := MatchAll[0];
    Result.DateTime := MatchAll[3] + ' ' + MatchAll[4] + ' ' + MatchAll[5];
    Result.Method := MatchAll[6];
    Result.ReqFile := MatchAll[7];
    Result.Code := StrToInt(MatchAll[9]);
    try
      Result.FileSize := StrToInt(MatchAll[10]);
    except
      Result.FileSize := 0;
    end;
    Result.Referrer := MatchAll[11];
    Result.UserAgent := MatchAll[12];
  end;
  RegExp.Free;
end;*)

function CalculateRuntime(s,e: TDateTime): String;
var
  ss: String;  
begin
  Result := '';
  DateTimeToString(ss,'hh',(e-s));
  Result := Result + ss + 'h ';
  DateTimeToString(ss,'nn',(e-s));
  Result := Result + ss + 'm ';
  DateTimeToString(ss,'ss',(e-s));
  Result := Result + ss + 's';    
end;

// Read, then write
procedure test2(logFile, dbPath: String);
var
  dbFile: String;
  db: TSQLite3Connection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  n: LongInt;
//  s: TStatsLine;
  f: TextFile;
  lineIn: String;
  i: integer;
  sItems: TList;
  pa: PStatsLine;
  RegExp: TBRRERegExp;
  MatchAll: TBRRERegExpStrings;
begin
  // TODO: Optimise this some more
  sItems := TList.Create;
  RegExp := TBRRERegExp.Create(Expression);
  // Create Database File
  n := DateTimeToUnix(Now);
  //dbFile := 'dbcache-'+IntToStr(n)+'.db';
  dbFile := ExtractFileName(logFile) + '.db';
  db := TSQLite3Connection.Create(nil);
  trans := TSQLTransaction.Create(nil);
  query := TSQLQuery.Create(nil);
  query.Database := db;
  query.Transaction := trans;
  db.Transaction := trans;
  db.Username := '';
  db.Password := '';
  db.DatabaseName := dbPath + dbFile;
  db.Open;
  trans.Active := true;
  db.ExecuteDirect('CREATE TABLE "stats" ("ip" Char(18), "datetime" Char(128), "method" Char(4), "ReqFile" Char(512), "Code" Integer, "Filesize" Integer, "Referrer" Char(1024), "Useragent" Char(1024))');
  trans.Commit;
  // Open log file and process
  AssignFile(f,logFile);
  Reset(f);
  i := 0;
  while not eof(f) do
  begin
    Readln(f,lineIn);
    inc(i);
    new(pa);
    MatchAll := RegExp.Split(lineIn);
    if Length(MatchAll) = 13 then
    begin
      pa^.Ip := MatchAll[0];
      pa^.DateTime := MatchAll[3] + ' ' + MatchAll[4] + ' ' + MatchAll[5];
      pa^.Method := MatchAll[6];
      pa^.ReqFile := MatchAll[7];
      try
        pa^.Code := StrToInt(MatchAll[9]);
      except
        pa^.Code := 0;
      end;
      try
        pa^.FileSize := StrToInt(MatchAll[10]);
      except
        pa^.FileSize := 0;
      end;
      pa^.Referrer := MatchAll[11];
      pa^.UserAgent := MatchAll[12];
    end;
    sItems.Add(pa);
  end;
  CloseFile(f);    
  trans.Active := false;
  trans.Action := caCommit;
  trans.StartTransaction;
  query.InsertSQL.Add('INSERT INTO "stats" VALUES (:ip,:datetime,:method,:ReqFile,:Code,:Filesize,:Referrer,:Useragent)');
  query.SQL.Text := 'SELECT * FROM "stats"';
  query.ReadOnly := false;
  query.Open;
  for i := 0 to sItems.Count -1 do
  begin  
    query.Insert;
    pa := sItems[i];
    query.FieldByName('ip').AsString := pa^.Ip;
    query.FieldByName('datetime').AsString := pa^.DateTime;
    query.FieldByName('method').AsString := pa^.Method;
    query.FieldByName('ReqFile').AsString := pa^.ReqFile;
    query.FieldByName('Code').AsInteger := pa^.Code;
    query.FieldByName('Filesize').AsInteger := pa^.FileSize;
    query.FieldByName('Referrer').AsString := pa^.Referrer;
    query.FieldByName('Useragent').AsString := pa^.UserAgent;
    query.Post;
    query.ApplyUpdates;      
  end;  
  writeln(i, ' records');  
  query.Close;
  trans.Active := false;
  db.Close;
  query.Free;
  trans.Free;
  db.Free;
  RegExp.Free;
  sItems.Free;
end;  

// Read, write together
(*procedure test1(logFile, dbPath: String);
var
  dbFile: String;
  db: TSQLite3Connection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  n: LongInt;
  //s: TStatsLine;
  f: TextFile;
  lineIn: String;
  i: integer; 
  RegExp: TBRRERegExp;
  MatchAll: TBRRERegExpStrings;  
begin
  RegExp := TBRRERegExp.Create(Expression);  
  // Create Database File
  n := DateTimeToUnix(Now);
  dbFile := 'dbcache-'+IntToStr(n)+'.db';
  db := TSQLite3Connection.Create(nil);
  trans := TSQLTransaction.Create(nil);
  query := TSQLQuery.Create(nil);
  query.Database := db;
  query.Transaction := trans;
  db.Transaction := trans;
  db.Username := '';
  db.Password := '';
  db.DatabaseName := dbPath + dbFile;
  db.Open;
  trans.Active := true;
  db.ExecuteDirect('CREATE TABLE "stats" ("ip" Char(18), "datetime" Char(128), "method" Char(4), "ReqFile" Char(512), "Code" Integer, "Filesize" Integer, "Referrer" Char(1024), "Useragent" Char(1024))'); 
  trans.Commit;  
  // Open log file and process
  AssignFile(f,logFile);
  Reset(f);
  i := 0;  
  trans.Active := false;
  trans.Action := caCommit;
  trans.StartTransaction;
  query.InsertSQL.Add('INSERT INTO "stats" VALUES (:ip,:datetime,:method,:ReqFile,:Code,:Filesize,:Referrer,:Useragent)');
  query.SQL.Text := 'SELECT * FROM "stats"';
  query.ReadOnly := false;
  query.Open;
  while not eof(f) do
  begin
    Readln(f,lineIn);
    inc(i);
    MatchAll := RegExp.Split(lineIn);    
    if Length(MatchAll) = 13 then
    begin
      query.Insert;          
      query.FieldByName('ip').AsString := MatchAll[0];
      query.FieldByName('datetime').AsString := MatchAll[3] + ' ' + MatchAll[4] + ' ' + MatchAll[5];
      query.FieldByName('method').AsString := MatchAll[6];
      query.FieldByName('ReqFile').AsString := MatchAll[7];
      query.FieldByName('Code').AsInteger := StrToInt(MatchAll[9]);
      try
        query.FieldByName('Filesize').AsInteger := StrToInt(MatchAll[10]);
      except
        query.FieldByName('Filesize').AsInteger := 0;
      end;
      query.FieldByName('Referrer').AsString := MatchAll[11];
      query.FieldByName('Useragent').AsString := MatchAll[12];
      query.Post;
      query.ApplyUpdates;            
    end;
  end;  
  writeln(i, ' records');  
  CloseFile(f);    
  query.Close;
  trans.Active := false;
  db.Close;    
  query.Free;
  trans.Free;
  db.Free;  
end;*)
  
begin  
  startTime := Now; 
  writeln('Apache log to sqlite3 database');
  writeln;
  if not FileExists(ParamStr(1)) then 
  begin
    writeln('Error: ',ParamStr(1),' not found!');
    exit;	    
  end;
  writeln('Processing ',ParamStr(1),'. Please wait');
  test2(ParamStr(1),'./');
  endTime := Now;
  writeln('runtime: ',CalculateRuntime(startTime,endTime));
end.
