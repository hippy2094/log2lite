unit miscfunc;

{$mode objfpc}{$H+}

interface

uses Classes, SysUtils, DateUtils;

type
  TArray = array of String;
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

function explode(cDelimiter,  sValue : string; iCount : integer) : TArray;
function MonthToNum(m: String): String;
function CalculateRuntime(s,e: TDateTime): String;

implementation

function explode(cDelimiter,  sValue : string; iCount : integer) : TArray;
var
  s : string; 
  i,p : integer;
begin
  s := sValue;
  i := 0;
  while length(s) > 0 do
  begin
    inc(i);
    SetLength(result, i);
    p := pos(cDelimiter,s);
    if ( p > 0 ) and ( ( i < iCount ) OR ( iCount = 0) ) then
    begin
      result[i - 1] := copy(s,0,p-1);
      s := copy(s,p + length(cDelimiter),length(s));
    end else
    begin
      result[i - 1] := s;
      s :=  '';
    end;
  end;
end;

// Rather nasty
function MonthToNum(m: String): String;
var
  Months: Array[1..12] of String;
  i,j: Integer;
  s: String;
begin
  Months[1] := 'Jan';
  Months[2] := 'Feb';
  Months[3] := 'Mar';
  Months[4] := 'Apr';
  Months[5] := 'May';
  Months[6] := 'Jun';
  Months[7] := 'Jul';
  Months[8] := 'Aug';
  Months[9] := 'Sep';
  Months[10] := 'Oct';
  Months[11] := 'Nov';
  Months[12] := 'Dec';
  for i := 1 to 12 do
  begin
    if m = Months[i] then j := i;
  end;
  if j < 10 then s := '0' + IntToStr(j)
  else s := IntToStr(j);
  Result := s;
end;

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

end.