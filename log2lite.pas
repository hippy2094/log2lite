program log2lite;

{$mode delphi}

uses Classes, SysUtils, DateUtils, sqldb, db, sqlite3ds, sqlite3conn, BRRE, BRREUnicode, miscfunc;

var
  startTime: TDateTime;
  endTime: TDateTime;

const
  Expression = '/^(\S+) (\S+) (\S+) \[([^:]+):(\d+:\d+:\d+) ([^\]]+)\] \"(\S+) (.*?) (\S+)\" (\S+) (\S+) "([^"]*)" "([^"]*)"$/';

// Read, then write
procedure PerformLogImport(logFile, dbPath: String);
var
  dbFile: String;
  db: TSQLite3Connection;
  trans: TSQLTransaction;
  query: TSQLQuery;
  f: TextFile;
  lineIn: String;
  i: integer;
  sItems: TList;
  pa: PStatsLine;
  RegExp: TBRRERegExp;
  MatchAll: TBRRERegExpStrings;
  dateParts: TArray;
  dateStr: String;
begin
  // TODO: Optimise this some more
  sItems := TList.Create;
  RegExp := TBRRERegExp.Create(Expression);
  // Create Database File
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
      dateParts := explode('/',MatchAll[3],0);
      dateStr := dateParts[2] + '-' + MonthToNum(dateParts[1]) + '-' + dateParts[0] + ' ' + MatchAll[4];
      pa^.DateTime := dateStr;
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

begin
  startTime := Now; 
  writeln('Apache log to sqlite3 database');
  writeln('(c) 2016, 2017 Matthew Hipkin <http://www.matthewhipkin.co.uk>');
  writeln;
  if not FileExists(ParamStr(1)) then
  begin
    writeln('Error: ',ParamStr(1),' not found!');
    exit;
  end;
  writeln('Processing ',ParamStr(1),'. Please wait');
  PerformLogImport(ParamStr(1),'.' + PathDelim);
  endTime := Now;
  writeln('runtime: ',CalculateRuntime(startTime,endTime));
end.
