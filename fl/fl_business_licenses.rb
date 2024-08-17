# Creator:      Sergii Butrymenko
# Dataset Name: Florida - Professional License Types (Florida Business Licenses)
# Task #:       6
# Migrated:     May 2021

# ruby mlc.rb --tool="clean::fl::fl_business_licenses"
# ruby mlc.rb --tool="clean::fl::fl_business_licenses" --mode='names'
# ruby mlc.rb --tool="clean::fl::fl_business_licenses" --mode='cities'


SOURCE_TABLE = 'new_florida_business_licenses'.freeze
CLEAN_TABLE = 'new_florida_business_licenses__names_clean'.freeze
RAW_ADDRESS_COL = 'address'.freeze
RAW_COMPL_ADDRESS_COL = 'complete_address'.freeze
CLEAN_ADDRESS_COL = 'address_clean'.freeze
RAW_CITY_COL = 'address_city'.freeze
CLEAN_CITY_COL = 'address_city_clean'.freeze
STATE_COL = 'address_state'.freeze
ORG_ID_COL = 'city_org_id'.freeze

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  where_part = options['where']
  mode = options['mode']&.to_sym

  case mode
  when :names
    names_cleaning(where_part, route)
  when :cities
    cities_cleaning(where_part, route)
  else
    names_cleaning(where_part, route)
    cities_cleaning(where_part, route)
  end
  route.close
end

def names_cleaning(where_part, route)
  recent_date = get_recent_date(route)
  names_to_clean = get_names_to_clean(recent_date, where_part, route)
  if names_to_clean.empty?
    message_to_slack("There is no any new names in the source table...")
    return
  end
  # det = Determiner.new
  names_to_clean.each do |row|
    clean_name = row
    # puts "#{clean_name['name']}".cyan
    # clean_name['name_type'] = det.determine(row['name'])
    # clean_name['name_cleaned'] = clean_name['name_type'] == 'Person' ? Cleaner.person_clean(row['name'].sub(/\.{2,}$/, '.'), false) : Cleaner.org_clean(row['name'].sub(/\.{2,}$/, '.')).sub(/^THE ARC /, 'The Arc ').sub(/^THE /, 'The ').gsub('. , ', '., ')
    result_name = row['name'].strip
    if result_name.start_with?('"') && result_name.end_with?('"')
      result_name = result_name[1..-1]
    end
    result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/^" /, '"').gsub(' ",', '",'))
    # puts "#{result_name}".yellow
    case result_name.count('"')
    when 1
      result_name = result_name.sub('"', '')
    when 2
      result_name = result_name.sub('", "', ', ')
    else
      nil
    end
    # puts "#{result_name}".cyan
    clean_name['name_cleaned'] = result_name.sub(/^"a /, '"A ').sub(/^THE /, 'The ').sub(/^A&a /, 'A&A ').gsub('. , ', '., ').gsub(', , ', ', ')
    # puts "#{clean_name['name_cleaned']}".cyan
    # puts JSON.pretty_generate(clean_name).yellow
    insert(route, CLEAN_TABLE, clean_name, true)
  end
end

def message_to_slack(message)
  Slack::Web::Client.new.chat_postMessage(
      channel: 'UKLB1JGDN',
      text: "*[CLEANING #6] Florida - Professional License Types* \n>#{message}",
      as_user: true
  )
end

def escape(str)
  case
  when str.nil?
    nil
  when str.class == Date
    str.to_s
  else
    str.gsub(/\\/, '\&\&').gsub(/'/, "''")
  end
  # return str if str.nil? || str.empty?
  # str = str.to_s
end

def insert(db, tab, h, ignore = false, log=false)
  query = <<~SQL
    INSERT #{ignore ? 'IGNORE ' : ''}INTO #{tab} (#{h.keys.map{|e| "`#{e}`"}.join(', ')})
    VALUES (#{h.values.map{|e|"'#{escape(e)}'"}.join(', ')});
  SQL
  p query if log
  db.query(query)
end

def get_names_to_clean(recent_date, where_part, route)
  query = <<~SQL
    SELECT src.name, MIN(last_scrape_date) AS scrape_date
    FROM #{SOURCE_TABLE} src
        LEFT JOIN #{CLEAN_TABLE} cl ON src.name=cl.name
    WHERE cl.name IS NULL
      AND src.name IS NOT NULL
      AND last_scrape_date>='#{recent_date}'
      #{"AND #{where_part}" if where_part}
    GROUP BY src.name;
  SQL
  puts query.green
  route.query(query).to_a
end

def get_recent_date(route)
  query = <<~SQL
    SELECT MAX(scrape_date) AS recent_date
    FROM #{CLEAN_TABLE};
  SQL
  puts query.green
  result = route.query(query).to_a.first['recent_date']
  if result.nil?
    Date.new(2020,1,1)
  else
    result
  end
end

######################### CITIES

def cities_cleaning(where_part, route_db01)
  route_db02 = C::Mysql.on(DB02, 'hle_resources')

  correct_bad_city_names(route_db01, route_db02)
  states_list = get_states_list(route_db01)
  if states_list.empty?
    message_to_slack('There is no any new city names to clean')
    return
  end
  # puts states_list
  states_list.each do |st|
    cities_to_clean = get_cities_to_clean(st[STATE_COL], route_db01)
    # puts cities_to_clean
    cities_to_clean.each do |row|
      clean_name = row
      clean_name[CLEAN_CITY_COL] = MiniLokiC::DataMatching::NearestWord.correct_city_name(row[RAW_CITY_COL].sub(/^E\.? /i, 'East ').sub(/^W\.? /i, 'West ').sub(/^N\.? /i, 'North ').sub(/^S\.? /i, 'South '), st[STATE_COL], 1)
      next if clean_name[CLEAN_CITY_COL].nil? || clean_name[CLEAN_CITY_COL].empty?
      clean_name[ORG_ID_COL] = get_city_org_id(st[STATE_COL], clean_name[CLEAN_CITY_COL], route_db02)
      # puts "#{clean_name[RAW_CITY_COL].rjust(30, ' ')} -- #{clean_name[CLEAN_CITY_COL]}".yellow
      update_cities(st[STATE_COL], clean_name, route_db01)
    end
  end
  address_cleaning(route_db01)
  route_db02.close
end

def get_states_list(route)
  query = <<~SQL
  SELECT DISTINCT address_state
  FROM #{SOURCE_TABLE}
  WHERE #{CLEAN_CITY_COL} IS NULL
    AND address_state IS NOT NULL
    AND address_state<>'';
  SQL
  # puts query.yellow
  route.query(query).to_a
end

def get_cities_to_clean(state, route)
  query = <<~SQL
  SELECT DISTINCT address_city
  FROM #{SOURCE_TABLE}
  WHERE #{CLEAN_CITY_COL} IS NULL
    AND address_state='#{state}'
  ORDER BY address_city;
  SQL
  puts query.magenta
  route.query(query).to_a
end

def update_cities(state, city_data, route)
  query = <<~SQL
  UPDATE #{SOURCE_TABLE}
  SET #{CLEAN_CITY_COL} = '#{escape(city_data[CLEAN_CITY_COL])}',
    #{ORG_ID_COL} = '#{city_data[ORG_ID_COL]}'
  WHERE address_city = '#{escape(city_data[RAW_CITY_COL])}'
    AND #{CLEAN_CITY_COL} IS NULL
    AND #{STATE_COL}='#{state}';
  SQL
  # puts query.green
  route.query(query).to_a
end

def get_city_org_id(state_code, city, route)
  query = <<~SQL
  SELECT pl_production_org_id
  FROM usa_administrative_division_counties_places_matching
  WHERE state_name=(SELECT name
                    FROM usa_administrative_division_states
                    WHERE short_name='#{state_code}')
    AND short_name='#{escape(city)}'
    AND bad_matching IS NULL
    AND pl_production_org_id IS NOT NULL
    AND short_name NOT IN (
          SELECT short_name
          FROM usa_administrative_division_counties_places_matching
          WHERE state_name=(SELECT name
                            FROM usa_administrative_division_states
                            WHERE short_name='#{state_code}')
            AND bad_matching IS NULL
          GROUP BY short_name
          HAVING count(short_name) > 1);
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    nil
  else
    res.first['pl_production_org_id']
  end
end

def correct_bad_city_names(route_db01, route_db02)
  bad_city_names = {
      "ATLAMONTE SPRING" => "Altamonte Springs",
      "AVENUTRA" => "Aventura",
      "BAL HARBOR" => "Bal Harbour",
      "BAL HARBOUR ISLAND" => "Bal Harbour",
      "BAY HARBOR" => "Bay Harbor Islands",
      "BAY HARBOR ISLAND" => "Bay Harbor Islands",
      "BAY HARBOUR" => "Bay Harbor Islands",
      "BEL HARBOUR" => "Bal Harbour",
      "BELLAIR BLUFFS" => "Belleair Bluffs",
      "BOCA ROTON" => "Boca Raton",
      "BONITAL SPRINGS" => "Bonita Springs",
      "BOYNTON" => "Boynton Beach",
      "BOYNTON BEACH FL" => "Boynton Beach",
      "BOYTON BEACH" => "Boynton Beach",
      "BRANDENTON" => "Bradenton",
      "BRANDETON" => "Bradenton",
      "BUSNELL" => "Bushnell",
      "CHIELFAND" => "Chiefland",
      "CITRUS SPNGS" => "Citrus Springs",
      "CLEAR WATER" => "Clearwater",
      "CLEARWATER BEACH" => "Clearwater",
      "CLERWATER" => "Clearwater",
      "COCO BEACH" => "Cocoa Beach",
      "COCUNUT CREEK" => "Coconut Creek",
      "COOPER CIRY" => "Cooper City",
      "COOPPER CITY" => "Cooper City",
      "CORA LSPRINGS" => "Coral Springs",
      "CORAL SPRING" => "Coral Springs",
      "CORAL SRPINGS" => "Coral Springs",
      "CRESTIEW" => "Crestview",
      "CUTTLER BAY" => "Cutler Bay",
      "DADECITY" => "Dade City",
      "DANIA" => "Dania Beach",
      "DANVENPORT" => "Davenport",
      "DAVE" => "Davie",
      "DAYTONA" => "Daytona Beach",
      "DAYTONA BCH" => "Daytona Beach",
      "DEEFIELD BEACH" => "Deerfield Beach",
      "DEERFIED BEACH" => "Deerfield Beach",
      "DEERFIELD BCH" => "Deerfield Beach",
      "DEERFILED BEACH" => "Deerfield Beach",
      "DEFUNIAK SPGS" => "De Funiak Springs",
      "DEFUNIAK SPRINGS" => "De Funiak Springs",
      "DEL RAY BEACH" => "Delray Beach",
      "DELARY BEACH" => "Delray Beach",
      "DELEON SPRINGS" => "De Leon Springs",
      "DELRAY BACH" => "Delray Beach",
      "DELROY BEACH" => "Delray Beach",
      "DONNELLON" => "Dunnellon",
      "DUNELLON" => "Dunnellon",
      "EUTIS" => "Eustis",
      "EVERGLADES CITY" => "Everglades",
      "FERNANDINA BCH" => "Fernandina Beach",
      "FERNINDINA BEACH" => "Fernandina Beach",
      "FORD MEADE" => "Fort Meade",
      "FORT LADUERDALE" => "Fort Lauderdale",
      "FORT LAUDEDALE" => "Fort Lauderdale",
      "FORT LAUDERDAALE" => "Fort Lauderdale",
      "FORT LAUDERDALE BY THE SEA" => "Lauderdale-by-the-Sea",
      "FORT LAUDERDALES" => "Fort Lauderdale",
      "FORT LAUDERHIL" => "Lauderhill",
      "FORT LLAUDERDALE" => "Fort Lauderdale",
      "FORT MEYERS" => "Fort Myers",
      "FORT MYERS FLORIDA" => "Fort Myers",
      "FORT PEIRCE" => "Fort Pierce",
      "FORT WALTON" => "Fort Walton Beach",
      "FORT-LAUDERDALE" => "Fort Lauderdale",
      "FORT. LAUDERDALE" => "Fort Lauderdale",
      "FORTLAUDERDALE" => "Fort Lauderdale",
      "FORTMYERS" => "Fort Myers",
      "FORTWALTON BEACH" => "Fort Walton Beach",
      "FOTT LAUDERDALE" => "Fort Lauderdale",
      "FROT LAUDERDALE" => "Fort Lauderdale",
      "FT LAUDERDALE" => "Fort Lauderdale",
      "FT LAUDERDALEL" => "Fort Lauderdale",
      "FT LAUDERDALES" => "Fort Lauderdale",
      "FT LUADERDALE" => "Fort Lauderdale",
      "FT MEADE" => "Fort Meade",
      "FT MYERS" => "Fort Myers",
      "FT PIERCE" => "Fort Pierce",
      "FT WALTON BEACH" => "Fort Walton Beach",
      "FT WHITE" => "Fort White",
      "FT. LAUDERDALE" => "Fort Lauderdale",
      "FT. MEADE" => "Fort Meade",
      "FT. MYERS" => "Fort Myers",
      "FT. PIERCE" => "Fort Pierce",
      "FT. WALTON BEACH" => "Fort Walton Beach",
      "FT. WHITE" => "Fort White",
      "FT.LAUDERDALE" => "Fort Lauderdale",
      "GAIBESVILLE" => "Gainesville",
      "GAINESVISILLE" => "Gainesville",
      "GAINEVILLE" => "Gainesville",
      "GAINSVILE" => "Gainesville",
      "GAINSVILLE" => "Gainesville",
      "GLEN SAINT MARY" => "Glen St. Mary",
      "GLEN ST MARY" => "Glen St. Mary",
      "GREEN COVE SPRING" => "Green Cove Springs",
      "GROVELED FL" => "Groveland",
      "GROVLAND" => "Groveland",
      "GULF PORT" => "Gulfport",
      "HALLANDALE" => "Hallandale Beach",
      "HALLANDALLE" => "Hallandale Beach",
      "HALLANDLAE BEACH" => "Hallandale Beach",
      "HALLANDLE BCH" => "Hallandale Beach",
      "HALLANDLE BEACH" => "Hallandale Beach",
      "HALLENDALE BEACH" => "Hallandale Beach",
      "HEALEAH" => "Hialeah",
      "HIAELAH" => "Hialeah",
      "HIALEAG" => "Hialeah",
      "HIALEAH GADENS" => "Hialeah Gardens",
      "HIALEAH GARDEN" => "Hialeah Gardens",
      "HIALEAH GRDS" => "Hialeah Gardens",
      "HIALEAHH" => "Hialeah",
      "HILEAH" => "Hialeah",
      "HOILDAY" => "Holiday",
      "HOLLYWOOD HILLS" => "Hollywood",
      "HOLLYWWOD" => "Hollywood",
      "HOMESTAED" => "Homestead",
      "HOMESTED" => "Homestead",
      "HOMSTEARD" => "Homestead",
      "HOWEY IN THE HILLS" => "Howey-in-the-Hills",
      "INDIALNTIC" => "Indialantic",
      "INDIAN CREEK VILLAGE" => "Indian Creek",
      "INDIDAN HARBOR BEACH" => "Indian Harbour Beach",
      "ISLAMORADA" => "Islamorada, Village of Islands",
      "JACKONVILLE" => "Jacksonville",
      "JACKOSNVILLE" => "Jacksonville",
      "JACKSINVILLE" => "Jacksonville",
      "JACKSON" => "Jacksonville",
      "JACKSON VILLE" => "Jacksonville",
      "JACKSONVILE" => "Jacksonville",
      "JACKSONVILLE FL 32244" => "Jacksonville",
      "JACKSOVILLE" => "Jacksonville",
      "JASKSONVILLE" => "Jacksonville",
      "JAX" => "Jacksonville",
      "JAX BEACH" => "Jacksonville Beach",
      "JENSEN BEACAH" => "Jensen Beach",
      "KEYWEST" => "Key West",
      "KISSIMME" => "Kissimmee",
      "KISSMMEE" => "Kissimmee",
      "KISSSIMMEE" => "Kissimmee",
      "Kissimee" => "Kissimmee",
      "LAAND OF LAKES" => "Land O'Lakes",
      "LAKE CLARK SHORES" => "Lake Clarke Shores",
      "LAKE WORTH BEACH" => "Lake Worth",
      "LAKEWORTH" => "Lake Worth",
      "LAKWORTH" => "Lake Worth",
      "LAND O LAKES" => "Land O'Lakes",
      "LAND O' LAKES" => "Land O'Lakes",
      "LAUDERDALE BY SEA" => "Lauderdale-by-the-Sea",
      "LAUDERDALE BY THE SEA" => "Lauderdale-by-the-Sea",
      "LAUDERDALE BY THESEA" => "Lauderdale-by-the-Sea",
      "LEEHIGH ACRES" => "Lehigh Acres",
      "LEGIGH ACRES" => "Lehigh Acres",
      "LEHICH ACRES" => "Lehigh Acres",
      "LEHIGH" => "Lehigh Acres",
      "LEHIGH ACERS" => "Lehigh Acres",
      "LEHIGH ACRESS" => "Lehigh Acres",
      "LEHIGH LEHIGH ACRES" => "Lehigh Acres",
      "LEIGH ACRES" => "Lehigh Acres",
      "LEIHIGH ACRES" => "Lehigh Acres",
      "LUADERHILL" => "Lauderhill",
      "MAIMI" => "Miami",
      "MARGETE" => "Margate",
      "MARRY ESTHER" => "Mary Esther",
      "MARUANNA" => "Marianna",
      "MCCLENNY" => "Macclenny",
      "MEDLY" => "Medley",
      "MELBOUNRE" => "Melbourne",
      "MELBOURE" => "Melbourne",
      "MERRITT IS" => "Merritt Island",
      "MIAIM BEACH" => "Miami Beach",
      "MIAM" => "Miami",
      "MIAMAR" => "Miramar",
      "MIAMI DADE" => "Miami",
      "MIAMI FL" => "Miami",
      "MIAMI FL 33135" => "Miami",
      "MIAMI FL 33183" => "Miami",
      "MIAMI GARDEN" => "Miami Gardens",
      "MIAMI LKES" => "Miami Lakes",
      "MIAMI-DADE" => "Miami",
      "MIRARAR" => "Miramar",
      "MIRMAR" => "Miramar",
      "MIRMAR BEACH" => "Miramar Beach",
      "MONTEVERDI" => "Montverde",
      "MT DORA" => "Mount Dora",
      "MT. DORA" => "Mount Dora",
      "MULLBERY" => "Mulberry",
      "N FORT MYERS" => "North Fort Myers",
      "N LAUDERDALE" => "North Lauderdale",
      "N MIAMI" => "North Miami",
      "N MIAMI BEACH" => "North Miami Beach",
      "N PALM BEACH" => "North Palm Beach",
      "N. MIAMI" => "North Miami",
      "N. MIAMI BEACH" => "North Miami Beach",
      "NAPES" => "Naples",
      "NCEVILLE" => "Niceville",
      "NEW PORT RICHIE" => "New Port Richey",
      "NEW PRT RCHY" => "New Port Richey",
      "NEW SMRYNA BEACH" => "New Smyrna Beach",
      "NEW SMYRNA" => "New Smyrna Beach",
      "NEW SYMRNA BEACH" => "New Smyrna Beach",
      "NEWPORT RICHEY" => "New Port Richey",
      "NO FORT MYERS" => "North Fort Myers",
      "NO. MIAMI BEACH" => "North Miami Beach",
      "NOKOMOS" => "Nokomis",
      "NORTH BAY VILALGE" => "North Bay Village",
      "NORTH FT MYERS" => "North Fort Myers",
      "NORTH FT. MYERS" => "North Fort Myers",
      "NORTH LAUDERDAE" => "North Lauderdale",
      "NORTH LAUDERDAL" => "North Lauderdale",
      "NORTHPORT" => "North Port",
      "OAKLAND PARK BLVD STE 101" => "Oakland Park",
      "OCOLEE" => "Ocoee",
      "ODESSSA" => "Odessa",
      "OKEECHOEBEE" => "Okeechobee",
      "OKLAND PARK" => "Oakland Park",
      "OPA LOCKA" => "Opa-locka",
      "OPA-LOCKA BLVD" => "Opa-locka",
      "OPA-LOKA" => "Opa-locka",
      "OPALOCKA" => "Opa-locka",
      "OPSREY" => "Osprey",
      "ORANDO" => "Orlando",
      "ORANGEPARK" => "Orange Park",
      "ORLADO" => "Orlando",
      "ORLAND" => "Orlando",
      "ORLANDI" => "Orlando",
      "ORMOND BCH" => "Ormond Beach",
      "PALM BEACH GARDEN" => "Palm Beach Gardens",
      "PALM HARBOR FL" => "Palm Harbor",
      "PALM SPRING" => "Palm Springs",
      "PALMBAY" => "Palm Bay",
      "PALMCOAST" => "Palm Coast",
      "PEMBKOKE PINES" => "Pembroke Pines",
      "PEMBROKE" => "Pembroke Pines",
      "PEMBROKE PINE" => "Pembroke Pines",
      "PEMRBOKE PINES" => "Pembroke Pines",
      "PENSASCOLA" => "Pensacola",
      "PINCREST" => "Pinecrest",
      "POMAPNO BEACH" => "Pompano Beach",
      "POMNAO BEACH" => "Pompano Beach",
      "POMPANO" => "Pompano Beach",
      "POMPANO BCH" => "Pompano Beach",
      "POMPANO BEECH" => "Pompano Beach",
      "PORT RICHIE" => "Port Richey",
      "PORT RICHY" => "Port Richey",
      "PORT SAINT LICIE" => "Port St. Lucie",
      "PORT SAINT LUCIE" => "Port St. Lucie",
      "PORT ST JOE" => "Port St. Joe",
      "PORT ST LUCIE" => "Port St. Lucie",
      "PORT ST LUICE" => "Port St. Lucie",
      "PORT ST.LUCIE" => "Port St. Lucie",
      "PSL" => "Port St. Lucie",
      "PT SAINT LUCIE" => "Port St. Lucie",
      "PT ST LUCIE" => "Port St. Lucie",
      "QUNICY" => "Quincy",
      "RIVERA BEACH" => "Riviera Beach",
      "RIVERIA BEACH" => "Riviera Beach",
      "ROYAL PALM BEACCH" => "Royal Palm Beach",
      "ROYAL PLM BCH" => "Royal Palm Beach",
      "S. MIAMI" => "South Miami",
      "S. PETERSBURG" => "St. Petersburg",
      "SAINT PETERS BURG" => "St. Petersburg",
      "SAINT PETERSBURG" => "St. Petersburg",
      "SAINT. PETERSBURG" => "St. Petersburg",
      "SARSAOTA" => "Sarasota",
      "SATELITE BEACH" => "Satellite Beach",
      "SEBERING" => "Sebring",
      "SEBRING FL" => "Sebring",
      "SEFNER" => "Seffner",
      "ST AUGUSTINE" => "St. Augustine",
      "ST CLOUD" => "St. Cloud",
      "ST JAMES CITY" => "St. James City",
      "ST PETE BEACH" => "St. Pete Beach",
      "ST PETERBURG" => "St. Petersburg",
      "ST PETERSBERG" => "St. Petersburg",
      "ST PETERSBUG" => "St. Petersburg",
      "ST PETERSBURG" => "St. Petersburg",
      "ST PETERSBUURG" => "St. Petersburg",
      "ST PETRSBURG" => "St. Petersburg",
      "ST. PETERSNURG" => "St. Petersburg",
      "ST.AUGUSTINE" => "St. Augustine",
      "ST.CLOUD" => "St. Cloud",
      "ST.PETERSBURG" => "St. Petersburg",
      "SUN CITY CETNER" => "Sun City Center",
      "SUNNYS ISLES BEACH" => "Sunny Isles Beach",
      "TALLAHASSA" => "Tallahassee",
      "TALLAHASSE" => "Tallahassee",
      "TALLAHASSEEE" => "Tallahassee",
      "TALLHASSEE" => "Tallahassee",
      "TAMAP" => "Tampa",
      "TAMAPA" => "Tampa",
      "TAMPAA" => "Tampa",
      "TARPON SPRING" => "Tarpon Springs",
      "TEMPLE TERRANCE" => "Temple Terrace",
      "THONOTOSASS" => "Thonotosassa",
      "TREASURE ISALND" => "Treasure Island",
      "W MELBOURNE" => "West Melbourne",
      "W PALM BEACH" => "West Palm Beach",
      "WELLINGTOPN" => "Wellington",
      "WESLEY CHAPER" => "Wesley Chapel",
      "WEST PALB BEACH" => "West Palm Beach",
      "WEST PALM /BEACH" => "West Palm Beach",
      "WEST PALM BCH" => "West Palm Beach",
      "WEST PALMBEACH" => "West Palm Beach",
      "WESTPALM BEACH" => "West Palm Beach",
      "WIAMAUMA" => "Wimauma",
      "WIMUAMA" => "Wimauma",
      "WINDEMERE" => "Windermere",
      "WINDER GARDEN" => "Winter Garden",
      "WINDEREMERE" => "Windermere",
      "WINTER GRDEN" => "Winter Garden",
      "WINTER SPINGS" => "Winter Springs",
      "WINTER SPRING" => "Winter Springs",
      "WINTERGARDEN" => "Winter Garden",
      "WINTERHAVEN" => "Winter Haven",
      "ZEPHRYHILLS" => "Zephyrhills",
      "jesen beach" => "Jensen Beach",
      "sunny isle beach" => "Sunny Isles Beach",
      "Hallandale" => "Hallandale Beach",
  }
  bad_city_names.each do |raw_name, clean_name|
    # puts "Updating #{raw_name.red} with #{clean_name.green}"
    org_id = get_city_org_id('FL', clean_name, route_db02)
    next if org_id.nil?
    query = <<~SQL
    UPDATE #{SOURCE_TABLE}
    SET #{CLEAN_CITY_COL}='#{escape(clean_name)}',
      #{ORG_ID_COL}='#{org_id}'
    WHERE #{RAW_CITY_COL}='#{escape(raw_name)}'
      AND #{STATE_COL}='FL'
      AND #{CLEAN_CITY_COL} IS NULL;
    SQL
    # puts query.cyan + "\n"
    route_db01.query(query)
  end
end

def address_cleaning(route)
  query = <<~SQL
    SELECT DISTINCT #{RAW_COMPL_ADDRESS_COL}, #{RAW_ADDRESS_COL}, #{CLEAN_CITY_COL}
    FROM #{SOURCE_TABLE}
    WHERE #{RAW_ADDRESS_COL} IS NOT NULL
     AND #{CLEAN_ADDRESS_COL} IS NULL
    LIMIT 10000;
  SQL
  address_list = route.query(query).to_a
  while address_list.empty? == false

    @semaphore = Mutex.new
    threads = Array.new(3) do
      Thread.new do
        thread_route = C::Mysql.on(DB01, 'usa_raw')
        loop do
          item = nil
          @semaphore.synchronize {
            item = address_list.pop
          }
          break if item.nil? && address_list.empty?
          # address_clean = Address.abbreviated_streets(item[RAW_ADDRESS_COL])
          # if address_clean.match?(/[a-z]/i) == false
          #   item[RAW_COMPL_ADDRESS_COL].split(item[CLEAN_CITY_COL]), route)
          # end
          query_up = <<~SQL
          UPDATE #{SOURCE_TABLE}
          SET #{CLEAN_ADDRESS_COL} = '#{escape(MiniLokiC::Formatize::Address.abbreviated_streets(item[RAW_ADDRESS_COL]))}'
          WHERE #{RAW_ADDRESS_COL} = '#{escape(item[RAW_ADDRESS_COL])}';
          SQL
          # puts query_up.green
          thread_route.query(query_up)
        end
        thread_route.close
      end
    end
    threads.each(&:join)
    address_list = route.query(query).to_a
  end
end
