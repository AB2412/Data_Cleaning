# Creator:      Sergii Butrymenko
# Dataset Name: Georgia Criminal Offenders
# Task #:       85
# Scrape Task:  https://lokic.locallabs.com/scrape_tasks/316
# Data Set:     https://lokic.locallabs.com/data_sets/259
# Created:      September 2022

# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders"
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='name'
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='institution'
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='city'
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='offense'
#
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='zip'
# ruby mlc.rb --tool="clean::ga::georgia_criminal_offenders" --mode='status'

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    name: {
      raw_table: 'georgia_criminal_offenders',
      clean_table: 'georgia_criminal_offenders__names_clean',
      raw_column: 'full_name',
      clean_column: 'full_name_clean',
    },
    institution: {
      raw_table: 'georgia_criminal_offenders',
      clean_table: 'georgia_criminal_offenders__institutions_clean',
      raw_column: 'most_recent_institution',
      clean_column: 'most_recent_institution_clean',
    },
    # zip: {
    #   raw_table: 'ny_newyork_bar',
    #   raw_column: 'law_firm_zip',
    #   clean_column: 'law_firm_zip5',
    # },
    city: {
      raw_table: 'georgia_criminal_offenders__institutions_clean',
      raw_column: 'city',
      clean_column: 'city_clean',
      state_column: 'state',
      org_id_column: 'city_org_id',
    },
    offense: {
      raw_table: 'georgia_criminal_offenders_offenses',
      clean_table: 'georgia_criminal_offenders_offenses_clean',
      raw_column: 'offense',
      clean_column: 'offense_clean',
    },
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :name
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_names(table_info, route)
  when :city
    # recent_date = get_recent_date(table_info, route)
    # fill_city_table(table_info, recent_date, where_part, route)
    clean_cities(table_info, route)
  when :offense
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_offenses(table_info, route)
  when :zip
    zip5(table_info, route)
  when :institution
    fill_institution_table(table_info, where_part, route)
    check_institutions(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def escape(str)
  return nil if str.nil?

  str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

def message_to_slack(message, type = '', to = 'one')
  type = case type
         when :alert
           ':error:'
         when :warning
           ':warning:'
         when :info
           ':information_source:'
         else
           ''
         end
  Slack::Web::Client.new.chat_postMessage(
    channel: 'UKLB1JGDN',
    text: "*[CLEANING #85] Georgia Criminal Offenders* \n>#{type} #{message}",
    as_user: true
  )
  if to == :all
    Slack::Web::Client.new.chat_postMessage(
      channel: 'U02A6JBK9P1',
      text: "*[CLEANING #85] Georgia Criminal Offenders* \n>#{type} #{message}",
      as_user: true
    )
  end
end

def get_recent_date(table_info, route)
  begin
    query = <<~SQL
      SELECT MAX(scrape_date) AS recent_date
      FROM #{table_info[:clean_table]};
    SQL
    puts query.green
    recent_date = route.query(query).to_a.first['recent_date']
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...")
    # local_connection = if table_info[:local_connection]
    #                      <<~SQL
    #                       employer_category VARCHAR(255),
    #                       emp_cat_fixed_manually BOOLEAN NOT NULL DEFAULT 0,
    #                       state VARCHAR(50) DEFAULT 'New Mexico',
    #                      SQL
    #                    else
    #                      nil
    #                    end
    constraints = "UNIQUE (#{table_info[:raw_column]})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:state_column]
      state = "#{table_info[:state_column]} VARCHAR(2),"
      city_org_id = "city_org_id BIGINT(20) DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:state_column]}, #{table_info[:raw_column]})"
    else
      state = nil
      city_org_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} 
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{table_info[:clean_column]} VARCHAR(255),
         #{type}
         #{state}
         #{city_org_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
         CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
         # CHARACTER SET latin1 COLLATE latin1_swedish_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    puts 'Table created'
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}".cyan
  recent_date
end

# def fill_city_table(table_info, recent_date, where_part, route)
#   query = <<~SQL
#     SELECT r.#{table_info[:raw_column]}, r.#{table_info[:state_column]}, MIN(DATE(r.created_at)) AS scrape_date
#     FROM #{table_info[:raw_table]} r
#       LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
#         AND r.#{table_info[:state_column]} = cl.#{table_info[:state_column]}
#     WHERE cl.#{table_info[:raw_column]} IS NULL AND cl.#{table_info[:state_column]} IS NULL
#       AND r.#{table_info[:raw_column]} IS NOT NULL AND r.#{table_info[:raw_column]}<>''
#       AND r.#{table_info[:state_column]} IS NOT NULL AND r.#{table_info[:state_column]}<>''
#       #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
#       #{"AND #{where_part}" if where_part}
#     GROUP BY r.#{table_info[:raw_column]}, r.#{table_info[:state_column]};
#   SQL
#   puts query.green
#   names_list = route.query(query).to_a
#   if names_list.empty?
#     message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables", :info
#   else
#     parts = names_list.each_slice(10_000).to_a
#     parts.each do |part|
#       insert_query = <<~SQL
#         INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, #{table_info[:state_column]}, scrape_date)
#         VALUES
#       SQL
#       part.each do |item|
#         insert_query << "('#{escape(item[table_info[:raw_column]])}','#{escape(item[table_info[:state_column]])}','#{item['scrape_date']}'),"
#       end
#       insert_query = "#{insert_query.chop};"
#       # puts insert_query.red
#       route.query(insert_query)
#     end
#   end
# end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl on r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date} 
      #{"AND #{where_part}" if where_part} 
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  if names_list.empty? || (names_list.size == 1 && names_list.first[table_info[:raw_column]].nil?)
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source tables"
  else
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}, scrape_date)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?
        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}',#{scrape_date}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

# def prepare_city(name)
#   name.sub(/^Last known -\s/i, '').sub(/^rural\s/i, '').sub(/\sTownship$/i, '')
# end

def clean_names(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  names_list = route.query(query).to_a
  if names_list.empty?
    puts "There is no any new names in *#{table_info[:clean_table]}* table."
    return
  end
  names_list.each do |row|
    clean_name = row
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    result_name = MiniLokiC::Formatize::Cleaner.person_clean(row[table_info[:raw_column]].dup)
    # Mc fix inside
    # result_name = result_name.sub(/(?<=[a-z])McH/, 'mch')
    # Mc fix inside
    # result_name = estate_of + ' ' + result_name if estate_of
    clean_name[table_info[:clean_column]] = result_name
    # puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
end

def clean_cities(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}, #{table_info[:state_column]}
    FROM #{table_info[:raw_table]}
    WHERE #{table_info[:clean_column]} IS NULL
      AND #{table_info[:raw_column]} IS NOT NULL
      AND #{table_info[:state_column]} = 'GA';
  SQL
  city_list = route.query(query).to_a
  return nil if city_list.empty?

  city_list.each do |item|
    puts JSON.pretty_generate(item).green
    city_data = item.dup

    # city_data[table_info[:clean_column]] = city_data[table_info[:raw_column]].split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    # city_name = prepare_city(city_data[table_info[:raw_column]]).split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    city_name = city_data[table_info[:raw_column]].
      sub(/\s+on\s+/i, '-on-').
      sub(/\bHL\b/i, 'Hill').
      sub(/\bHTS\b/i, 'Heights').
      sub(/\bJCT\b/i, 'Junction').
      sub(/\bSPG\b/i, 'Springs').
      split(',')[0].split(/\b/).map(&:capitalize).join.sub("'S", "'s")
    correct_city_name = MiniLokiC::DataMatching::NearestWord.correct_city_name(city_name, city_data[table_info[:state_column]], 1) if city_name.length > 5
    if correct_city_name.nil?
      city_name = MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city_name.dup))
    else
      city_name = correct_city_name
    end
    puts city_name.black.on_red
    # if city_data[table_info[:state_column]] == 'NY'
    #   fixed_city_name = city_name.dup.downcase.sub(/.+/, city_hash)
    #   city_name = fixed_city_name unless fixed_city_name.empty?
    # end

    city_name, city_org_id = get_city_org_id(city_data[table_info[:state_column]], city_name, route)
    puts "#{city_data[table_info[:raw_column]]} -- #{city_name} -- #{city_org_id}".yellow
    query = <<~SQL
      UPDATE #{table_info[:raw_table]}
      SET #{table_info[:clean_column]} = '#{escape(city_name)}',
        #{table_info[:org_id_column]} = #{city_org_id.nil? ? "NULL" : "#{city_org_id}"}
      WHERE #{table_info[:raw_column]} = '#{escape(item[table_info[:raw_column]])}'
        AND #{table_info[:state_column]}='#{item[table_info[:state_column]]}'
        AND #{table_info[:clean_column]} IS NULL;
    SQL
    puts query.red
    route.query(query)
  end
end

def get_city_org_id(state_code, city, route)
  query = <<~SQL
    SELECT id, short_name, pl_production_org_id
    FROM hle_resources_readonly_sync.usa_administrative_division_counties_places_matching
    WHERE state_name=(SELECT name
                      FROM hle_resources_readonly_sync.usa_administrative_division_states
                      WHERE short_name='#{state_code}')
      AND short_name='#{escape(city)}'
      AND bad_matching IS NULL
      AND has_duplicate=0
      AND pl_production_org_id IS NOT NULL;
  SQL
  # puts query.green
  res = route.query(query).to_a
  if res.empty? || res.count > 1
    [city, 'NULL']
  else
    [res.first['short_name'], res.first['pl_production_org_id']]
  end
end

def fill_institution_table(table_info, where_part, route)
  # LEFT JOIN #{table_info[:clean_table]} cl ON IFNULL(r.#{table_info[:raw_status_column]}, '~~~')=IFNULL(cl.#{table_info[:raw_status_column]}, '~~~')
  #   AND IFNULL(r.#{table_info[:raw_type_column]}, '~~~')=IFNULL(cl.#{table_info[:raw_type_column]}, '~~~')
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]}=cl.#{table_info[:raw_column]}
    WHERE cl.id IS NULL
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  institution_list = route.query(query).to_a
  if institution_list.empty? || institution_list.first.nil?
    message_to_slack "No new records for *#{table_info[:raw_column]}* column found in source table *#{table_info[:raw_table]}*"
  else
    parts = institution_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]})
        VALUES
      SQL
      part.each do |item|
        # next if item[table_info[:raw_column]].nil?

        insert_query << "(#{escape_or_null(item[table_info[:raw_column]])}),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
    message_to_slack "#{institution_list.size} statuses were added into *db01.usa_raw.#{table_info[:clean_table]}* and should be cleaned by editors."
  end
end

def check_institutions(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  status_list = route.query(query).to_a
  message = if status_list.empty?
              "There is no any new institution in *#{table_info[:clean_table]}* table to clean."
            else
              "#{status_list.size} institution(s) in *db01.lawyer_status.#{table_info[:clean_table]}* aren't cleaned."
            end
  message_to_slack message
end

def clean_offenses(table_info, route)
  to_downcase = %w[a an and as at by for from in of or on over out the to under with without]
  offenses_replace = {
    /\bW\/O\b/i => 'without ',
    /\bW\/?(?:\b|\s)/i => 'with ',
    /\b1ST OFFENDER\b/i => 'a first-time offender',
    /\b1ST( DEGREE)?\b/i => 'in the first degree',
    /\b2ND( DEGREE)?\b/i => 'in the second degree',
    /\b3RD( DEGREE)?\b/i => 'in the third degree',
    /\b4th( DEGREE)?\b/i => 'in the forth degree',
    /\bFinan Ident Frd\b/i => 'Financial Identity Fraudulently',
    /\bCntrbtng Delinqency\b/i => 'Contributing to the Delinquency',
    /\b(ATT|ATMPT|ATTM)\b/i => 'attempted',
    /\bABANDMNT\b/i => 'Abandonment',
    /\bTRAF(FICK)?\b/i => 'trafficking',
    /\bMETH(AMPH)?\b/i => 'methamphetamine',
    /\bP(OSS)?\b/i => 'possession',
    /\bDUR\b/i => 'during',
    /\bFAIL REGISTR\b/i => 'failing to register',
    /\bINT DIST?\b/i => 'intent to distribute',
    /\bINT\b/i => 'intent',
    /\bINTNT\b/i => 'intention',
    /\bCONVCT\b/i => 'convicted',
    /\bFELON\b/i => 'felony',
    /\bAGG(RAV)?\b/i => 'aggravated',
    /\bRECV?\b/i => 'receiving',
    /\bPROPY?\b/i => 'property',
    /\bBURG\b/i => 'burglary',
    /\bSEXL?\b/i => 'sexual',
    /\bDEP\b/i => 'deportation',
    /\bSTIM\b/i => 'stimulant',
    /\bCNTRFT?\b/i => 'counterfeit',
    /\bPBLIC H\b/i => 'public health',
    /\bPEACE OFCR\b/i => 'of a peace officer',
    /\bSAFTY\b/i => 'safety',
    /\bAMPHTMINE\b/i => 'amphetamine',
    /\bCRML ATMPT FELONY\b/i => 'criminal attempt',
    /\bFORG\b/i => 'forgery',
    /\bSNATCH\b/i => 'snatching',
    /\bMOTORVEH\b/i => 'motor vehicle',
    /\bMANUFAC\b/i => 'manufacturing',
    /\bAS(S|LT)\b/i => 'Assault',
    /\bCNSPIRE\b/i => 'conspire',
    /\bConsprcy\b/i => 'Conspiracy',
    /\b(CNTRL|CONTRLD)\b/i => 'controled',
    /\bSUB(S?T)?|Substnc\b/i => 'substance',
    /\bVIO(L(ATN)?)?\b/i => 'violation',
    /\b(dang|DNG(ROUS)?)\b/i => 'dangerous',
    /\bDRGS\b/i => 'drugs',
    /\bOBSTR\b/i => 'obstruction',
    /\bENF\b/i => 'enforcement',
    /\b(MANF|MFG|MANUFACT)\b/i => 'manufacture',
    /\bMISC\b/i => 'miscellaneous',
    /\bCORRECTIONL\b/i => 'Correctional',
    /\bINST\b/i => 'institution',
    /\bFALSE STATEMENTS GOVT\b/i => 'false statements to the government',
    /\bGOVT\b/i => 'Government',
    /\b(CRMNL|CRML|Crim)\b/i => 'criminal',
    /\bEXTSY\b/i => 'extasy',
    /\bMARIJNA\b/i => 'marijuana',
    /\bNARCOTIC\b/i => 'narcotics',
    /\bBRING\b/i => 'bringing',
    /\bOTHR\b/i => 'other',
    /\bDISAB\b/i => 'disabled',
    /\b(CONTR(oled|lld)|Ctrl)\b/i => 'controlled',
    /\bCommt\b/i => 'Commit ',
    /\bConcld\b/i => 'Сoncealed',
    /\bRECRD\b/i => 'recorded',
    /\bOFF(en)?\b/i => 'offense',
    /\bPISTL\b/i => 'pistol',
    /\bOBT\b/i => 'obtain',
    /\b(CONVSN|Conversn)\b/i => 'conversion',
    /\b(PAYMNTS|Paymnt)\b/i => 'payments',
    /\b(UNLWFL|Unlaw)\b/i => 'Unlawful',
    /\bDEL\b/i => 'delivery',
    /\bUNAUTH DIST\b/i => 'Unauthorized distribution',
    /\bDIST?\b/i => 'distribute',
    /\bUNAUTH\b/i => 'unauthorized',
    /\bN-C S\b/i => ' non-controlled substance',
    /\bHOM\b/i => 'homicide',
    /\bS\/D\b/i => 'sales/distribution',
    /\bPURP\b/i => 'purpose',
    /\bINDEC\b/i => 'indecent',
    /\bSOLICIT\b/i => 'solicitation',
    /\bUNSPEC\b/i => 'unspecified',
    /\bAMT\b/i => 'amount',
    /\bCARRY\b/i => 'carrying',
    /\bLICNS\b/i => 'license',
    /\bExhib(it)?\b/i => 'exhibition',
    /\bMOLEST\b/i => 'molestation',
    /\bCH\b/i => 'child',
    /\bMat(er|rl)?\b/i => 'material',
    /\bWPN\b/i => 'weapon',
    /\bPRISNR\b/i => 'prisoner',
    /\bREMOV\b/i => 'removal',
    /\bPERS\b/i => 'person',
    /\bCSTDY\b/i => 'custody',
    /\bPEEPING TOM\b/i => 'Spying on/invading the privacy of another',
    /\bOBTND\b/i => 'obtained',
    /\bFRAUD\b/i => 'fraudulently',
    /\b(SRVS|SVC)\b/i => 'services',
    /\bGDS\b/i => 'goods',
    /\bDRV(NG)?\b/i => 'driving',
    /\b(HABTL|HABIT)\b/i => 'habitual',
    /\bWRITTN\b/i => 'writing',
    /\bSTMT\b/i => ' statement',
    /\bCOMM\b/i => 'communication',
    /\bUSE\b/i => 'Using',
    /\bCPWL\b/i => 'carrying a pistol without a license',
    /\bdui\b/i => 'driving under the influence',
    /\bimprop\b/i => 'improper',
    /\bveh\b/i => 'vehicle',
    /\breg\b/i => 'registration',
    /\bGA\b/i => 'Georgia',
    /\bRECK\b/i => 'reckless',
    /\bCOND\b/i => 'conduct',
    /\bBIGAMY\b/i => 'Bigamy',
    /\bPERJURY\b/i => 'Perjury',
    /\bID\b/i => 'identification',
    /\bALT\b/i => 'altered',
    /\bVOL\b/i => 'voluntary',
    /\bMRALS\b/i => 'morals',
    /\bMinr\b/i => 'Minor',
    /\bCommerical\b/i => 'Commercial',
    /\bMisd(emnor)?\b/i => 'Misdemeanor',
    /\b(Intfer|INTERF)\b/i => 'Interference',
    /\bDistr(but)?\b/i => 'Distributing',
    /\bInstrmntalties\b/i => 'Instrumentalities',
    /\bPrac\b/i => 'Practices',
    /\bSusp\b/i => 'Suspended',
    /\bEntice\b/i => 'Enticement',
    /\bEntrng\b/i => 'Entering',
    /\b(Mot|Motr)\b/i => 'Motor',
    /\bExploit\b/i => 'Exploitation',
    /\bImpris\b/i => 'Imprisonment',
    /\bSwearng\b/i => 'Swearing',
    /\bCompute\b/i => 'Computer',
    /\b(Er|Emer)\b/i => 'Emergency',
    /\bHinder\b/i => 'Hindering',
    /\bAppreh\b/i => 'Apprehension',
    /\bPun\b/i => 'Punishment',
    /\bHit-Run\b/i => 'Hit-and-Run',
    /\bIllgl\b/i => 'Illegal',
    /\bImpersntng\b/i => 'Impersonating',
    /\bEquip\b/i => 'Equipment',
    /\b(Dvce|Devise)\b/i => 'Device',
    /\bKeepng\b/i => 'Keeping',
    /\bPub Offl\b/i => 'Public Official',
    /\bPub\b/i => 'Public',
    /\bPriv\b/i => 'Private',
    /\bTrsps\b/i => 'trespass',
    /\bDam\b/i => 'Damage',
    /\bOffs\b/i => 'Offenses',
    /\bAdm\b/i => 'Administration',
    /\bObtn\b/i => 'Obtain',
    /\bAttmpt\b/i => 'Attempting',
    /\bIllegly\b/i => 'Illegally',
    /\bAgn(st)?\b/i => 'Against',
    /\bHlth\b/i => 'Health',
    /\bAlc\b/i => 'Alcohol',
    /\bBev\b/i => 'Beverage',
    /\bRel\b/i => 'Related',
    /\bProv\b/i => 'Providing',
    /\bPl\b/i => 'Plate',
    /\balcoh\b/i => 'Alcoholic',
    /\bregs\b/i => 'Regulations',
    /\breq\b/i => 'Request ',
    /\bSvc\b/i => 'Services',
    /\bFACLTY\b/i => 'Facility',
    /\bTrnsatns\b/i => 'Transactions',
    /\bProcd\b/i => 'Procedure',
    /\bMED\b/i => 'Medical',
    /\bPROF\b/i => 'Professional',
    /\bOffcr\b/i => 'Officer',
    /\bSals\b/i => 'Sales',
    /\bAbandon\b/i => 'Abandonment of',
    /\bIdent\b/i => 'Identity',
  }
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  offenses_list = route.query(query).to_a
  if offenses_list.empty?
    puts "There is no any new _#{table_info[:raw_column]}_ in *#{table_info[:clean_table]}* table."
    return
  end
  offenses_list.each do |row|
    clean_name = row
    prepared_name = row[table_info[:raw_column]].dup.split(/\b(BEF|AFT)\b/i).first.chomp(' ')
    offenses_replace.each do |key, value|
      prepared_name.gsub!(key, value)
    end
    clean_name[table_info[:clean_column]] = prepared_name.split(/\b/).map{|i| to_downcase.include?(i.downcase) ? i.downcase : i.capitalize}.join.gsub(/'S\b'/, "'s").gsub(/\bLsd\b/, 'LSD').gsub(/\bMda\b/, 'MDA').gsub(/\bHiv\b/, 'HIV').squeeze(' ')
    puts "#{clean_name[table_info[:raw_column]]}".cyan
    puts JSON.pretty_generate(clean_name).yellow
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
    SQL
    route.query(update_query)
  end
  message_to_slack("*#{offenses_list.count}* crimes were added to *db01.usa_raw.#{table_info[:clean_table]}* table and cleaned but should be reviewed by editors.", :warning, :all)
end

# def add_point(name)
#   words_list = name.split
#   words_list.each_with_index do |word, index|
#     if word.match(/^[A-Z]$/) && words_list[index + 1]&.downcase != 'professional'
#       words_list[index] = "#{word}."
#     end
#   end
#   words_list.join(' ')
# end
