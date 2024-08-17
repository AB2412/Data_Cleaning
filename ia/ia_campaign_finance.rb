# Creator:      Sergii Butrymenko
# Dataset Name: Iowa State Campaign Finance
# Task #:       94
# Migrated:     March 2023

# ruby mlc.rb --tool="clean::ia::ia_campaign_finance" --mode='committee'
# ruby mlc.rb --tool="clean::ia::ia_campaign_finance" --mode='contributor'
#
# ruby mlc.rb --tool="clean::ia::ia_campaign_finance" --mode='cand_payee'
# ruby mlc.rb --tool="clean::ia::ia_campaign_finance" --mode='cmte_contributor'
# ruby mlc.rb --tool="clean::ia::ia_campaign_finance" --mode='cmte_payee'

BAD_COUNTIES = ['_CA', '_Multiple Counties', '_NA', 'Multiple Counties', 'NA'].freeze
BAD_CITY_STATE_ZIP = ['anywhere', 'n/a', 'NA', 'dm-nodata', 'WDM', '-', '--', '---', 'Unknown', 'dbq', 'DSM', 'none', 'CR', 'X'].freeze
BAD_ADDRESS = ['streets', '123', '123 street', 'n/a, n/a', 'NA', 'NA NA', 'N/A', 'n/a', 'dm-nodata', 'unk', 'unknown', 'xxxxxx', '-', '--', '---', 'None', 'Main St.', 'Po Box'].freeze

def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_raw')
  table_description = {
    committee: {
      raw_table: 'IECDB_iowa_campaign_disclosures_committees',
      clean_table: 'IECDB_iowa_campaign_disclosures_committees__matched',
    },
    contributor: [
      # first_name
        {
        raw_table: 'IECDB_iowa_campaign_disclosures_contributions',
        clean_table: 'IECDB_iowa_campaign_disclosures_contributions__first_names_clean',
        raw_column: 'contributor_first_name',
        clean_column: 'contributor_first_name_clean'
      },
      # last_name
      {
        raw_table: 'IECDB_iowa_campaign_disclosures_contributions',
        clean_table: 'IECDB_iowa_campaign_disclosures_contributions__last_names_clean',
        raw_column: 'contributor_last_name',
        clean_column: 'contributor_last_name_clean'
      },
      # organization:
      {
        raw_table: 'IECDB_iowa_campaign_disclosures_contributions',
        clean_table: 'IECDB_iowa_campaign_disclosures_contributions__orgs_clean',
        raw_column: 'contributing_organization',
        clean_column: 'contributing_organization_clean'
      }
    ],
    vendor: [
      # first_name
      {
        raw_table: 'IECDB_iowa_campaign_disclosures_expenditures',
        clean_table: 'IECDB_iowa_campaign_disclosures_expenditures__first_names_clean',
        raw_column: 'received_first_name',
        clean_column: 'received_first_name_clean'
      },
      # last_name
      {
        raw_table: 'IECDB_iowa_campaign_disclosures_expenditures',
        clean_table: 'IECDB_iowa_campaign_disclosures_expenditures__last_names_clean',
        raw_column: 'received_last_name',
        clean_column: 'received_last_name_clean'
      },
      # organization:
      {
        raw_table: 'IECDB_iowa_campaign_disclosures_expenditures',
        clean_table: 'IECDB_iowa_campaign_disclosures_expenditures__orgs_clean',
        raw_column: 'receiving_organization_name',
        clean_column: 'receiving_organization_name_clean'
      }
    ]
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :committee
    clean_committee_info(table_info, route)
    check_unmatched(table_info, route)
  when :contributor
    table_info.each do |info|
      names_cleaning(info, where_part, route)
    end
  #
  #   recent_date = get_recent_date(table_info, route)
  #   fill_table(table_info, recent_date, where_part, route)
  #   clean_mixed_names(table_info, route)
  else
    puts 'EMPTY'.black.on_yellow
  end
  route.close
end

def message_to_slack(message, type = '')
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
    text: "*[CLEANING #94] Iowa State Campaign Finance* \n>#{type} #{message}",
    as_user: true
  )
end

def escape(str)
  # str = str.to_s.strip.squeeze(' ')
  return str if str.nil?

  str = str.to_s
  str.gsub(/\\/, '\&\&').gsub(/'/, "''")
end

def escape_or_null(str)
  return 'NULL' if str.nil?

  "'#{str.to_s.gsub(/\\/, '\&\&').gsub(/'/, "''")}'"
end

# Cleaning

def names_cleaning(table_info, where_part, route)
  recent_date = get_recent_date(table_info, route)
  fill_table(table_info, recent_date, where_part, route)
  clean_any_names(table_info, route, table_info[:raw_column] != 'contributing_organization')
end

def clean_committee_info(table_info, route)
  bad_counties_regexp = /\b#{Regexp.union(BAD_COUNTIES)}\b/i
  committee_data = get_committee_data(table_info, route)
  # puts committee_data
  committee_data.each do |item|
    puts JSON.pretty_generate(item).yellow
    committee_name = clean_committee_name(item[:committee_name])
    candidate_name = item[:candidate_name].dup
    candidate_name = fix_encode(item[:candidate_name], candidate_name, 'IECDB_iowa_campaign_disclosures_committees__matched') if !candidate_name.nil? && candidate_name.match?(/[^\x00-\x7F]{2}/)
    candidate_name = MiniLokiC::Formatize::Cleaner.person_clean(candidate_name, false)
    county = item[:county].match?(bad_counties_regexp) ? '' : item[:county].split(/\b/).map(&:capitalize).join
    city, state, zip, address, address_type = extract_city_state_zip_address(item)
    # address = MiniLokiC::Formatize::Address.abbreviated_streets(item[:address])
    # puts "CMTE NAME: #{committee_name}"
    # puts "CAND NAME: #{candidate_name}"
    # puts "CITY: #{city}"
    # puts "STATE: #{state}"
    # puts "ZIP: #{zip}"
    # puts "ADDRESS: #{address}"
    address_type = if address_type.nil?
                     'no_address'
                   else
                     %w[candidate_address contact_address treasurer_address][address_type - 1]
                   end
    insert_query = <<~SQL
      INSERT IGNORE INTO IECDB_iowa_campaign_disclosures_committees__matched
      (committee_number, committee_name, committee_name_clean, candidate_name, candidate_name_clean, county, address, city, state, zip, address_type)
      VALUES
      ('#{item[:committee_number]}', #{escape_or_null(item[:committee_name])}, #{escape_or_null(committee_name)}, #{escape_or_null(item[:candidate_name])}, #{escape_or_null(candidate_name)}, #{escape_or_null(county)}, #{escape_or_null(address)}, #{escape_or_null(city)}, #{escape_or_null(state)}, #{escape_or_null(zip)}, #{escape_or_null(address_type)});
    SQL
    # puts insert_query
    route.query(insert_query)
  end
end

def get_committee_data(table_info, route)
  # First, use candidate_city_state_zip (supposing it is in the state)
  # But, I think candidate and contact address info should be pretty safe to use.
  # The chair, if it is in Iowa and looks good, would be a good third choice.
  # And, skip any that are not in Iowa.
  query = <<~SQL
    SELECT c.committee_number,
           c.committee_name,
           c.candidate_name,
           c.county,
           c.candidate_address AS address1,
           c.candidate_city_state_zip AS city_state_zip1,
           c.contact_address AS address2,
           c.contact_city_state_zip AS city_state_zip2,
           c.treasurer_address AS address3,
           c.treasurer_city_state_zip AS city_state_zip3
    FROM IECDB_iowa_campaign_disclosures_committees c
      LEFT JOIN IECDB_iowa_campaign_disclosures_committees__matched m
        ON c.committee_number = m.committee_number
    WHERE m.id IS NULL;
  SQL
  # query = <<~SQL
  #   SELECT c.committee_number,
  #          c.committee_name,
  #          c.candidate_name,
  #          c.county,
  #          IF(c.contact_city_state_zip IS NOT NULL AND
  #             c.contact_city_state_zip NOT LIKE 'anywhere%' AND
  #             c.contact_address NOT LIKE '0%' AND
  #             c.contact_city_state_zip <> '', c.contact_address,
  #             c.candidate_address)      AS address,
  #          IF(c.contact_city_state_zip IS NOT NULL AND
  #             c.contact_city_state_zip NOT LIKE 'anywhere%' AND
  #             c.contact_address NOT LIKE '0%' AND
  #             c.contact_city_state_zip <> '', c.contact_city_state_zip,
  #             c.candidate_city_state_zip) AS city_state_zip
  #   FROM IECDB_iowa_campaign_disclosures_committees c
  #   LEFT JOIN IECDB_iowa_campaign_disclosures_committees__matched m
  #       ON c.committee_number=m.committee_number
  #   WHERE m.id IS NULL;
  # SQL
  puts query.green
  retry_count = 0
  begin
    committee_data = route.query(query, symbolize_keys: true).to_a
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    create_query = <<~SQL
      CREATE TABLE `IECDB_iowa_campaign_disclosures_committees__matched` (
        `id` int(11) AUTO_INCREMENT PRIMARY KEY,
        `committee_number` varchar(255) NOT NULL,
        `committee_name` varchar(255) NOT NULL,
        `committee_name_clean` varchar(255) DEFAULT NULL,

        `candidate_name` varchar(255) DEFAULT NULL,
        `candidate_name_clean` varchar(255) DEFAULT NULL,
        `county` varchar(255) DEFAULT NULL,

        `address` varchar(255) DEFAULT NULL,
        `city` varchar(255) DEFAULT NULL,
        `state` varchar(2) DEFAULT NULL,
        `zip` varchar(10) DEFAULT NULL,
        `address_type` varchar(20) DEFAULT NULL,

        `created_at` timestamp DEFAULT CURRENT_TIMESTAMP,
        `updated_at` timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        `created_by` varchar(255) NOT NULL DEFAULT 'Sergii Butrymenko',
        UNIQUE KEY `committee_number` (`committee_number`))
        # KEY `updated_at` (`updated_at`)
        CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    if retry_count > 0
      message_to_slack("Can't create clean table *#{table_info[:clean_table]}*. Exiting...", :alert)
      return
    else
      puts create_query.red
      route.query(create_query)
      retry_count += 1
      retry
    end
  end
  committee_data
end

def clean_committee_name(committee_name)
  result_name = committee_name.dup.sub(/\bCTE\b/, 'Committee to Elect').gsub(/[“”]/, '"').gsub('’', "'").strip
  while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
    result_name = result_name[1..-1]
  end
  result_name = fix_encode(committee_name, result_name, 'IECDB_iowa_campaign_disclosures_committees__matched') if result_name.match?(/[^\x00-\x7F]{2}/)

  MiniLokiC::Formatize::Cleaner.org_clean(result_name.sub(/ C\/O.+/i, '').sub('&amp;', '&').sub('&#39;', "'").sub(/\bCOM\b/i, 'Committee').sub(/\bST REP\b/i, 'State Representative').sub(/\bSTATE REP\b/i, 'State Representative').sub(/\bDEM\b/i, 'Democratic').sub(/\bREP\b/i, 'Republican').sub(/\bCO\b/i, 'County').sub(/\bEMPL\b/i, 'Employee').sub(/\bEMPLS\b/i, 'Employees').sub(/\bGOV\b/i, 'Government').sub(/\bLEG\b/i, 'Legislative').sub(/\bCAMP\b/i, 'Campaign').sub(/\bTWP\b/i, 'Township').sub(/\bPOL\b/i, 'Political').sub(/\bFED\b/i, 'Federation').sub(/\bWRKRS\b/i, 'Workers').sub(/\bia\b/i, 'Iowa').strip.squeeze(' '))
  # puts JSON.pretty_generate(result_name).yellow
end

def fix_encode(raw_name, name, table)
  result_name = name.encode("Windows-1252").force_encoding("UTF-8").gsub(/[“”]/, '"').gsub('’', "'")
  message_to_slack("Check this name >> #{raw_name} << with >> #{result_name} << name.encode(\"Windows-1252\").force_encoding(\"UTF-8\") from *#{table}* table.", :warning)
  result_name
  rescue Encoding::UndefinedConversionError => exception
    puts "Error during processing: #{$!}"
    puts "Backtrace:\n\t#{exception.backtrace.join("\n\t")}"
    message_to_slack("Error during processing: #{$!}\n\n```#{exception.backtrace.join("\n")}```", :alert)
end

def extract_city_state_zip_address(data)
  (1..3).each do |i|
    address_sym = "address#{i}".to_sym
    city_state_zip_sym = "city_state_zip#{i}".to_sym
    next if data[city_state_zip_sym].nil?

    city, state, zip = split_city_state_zip(data[city_state_zip_sym])

    puts '=============================================================='
    puts state
    puts data[city_state_zip_sym]
    puts data[city_state_zip_sym]
    puts '=============================================================='

    if state == 'IA' && !data[city_state_zip_sym].empty?  && !data[address_sym].empty? && !data[city_state_zip_sym].match?(/^#{Regexp.union(BAD_CITY_STATE_ZIP)}$/i) && !data[address_sym].match?(/^#{Regexp.union(BAD_ADDRESS)}$/i)
      address = MiniLokiC::Formatize::Address.abbreviated_streets(data[address_sym])
      puts i
      return [city, state, zip, address, i]
    end
  end
  Array.new(5)
end

def split_city_state_zip(city_state_zip)
  # m = city_state_zip.match(/(?<city>.*[a-z])\W+(?<state>[a-z]{2})\W+(?<zip>\d*-?\d+)\W*$/i)
  m = city_state_zip.match(/(?<city>.*\w)\W+(?<state>[a-z]{2})\W+(?<zip>\d*-?\d+)\W*$/i)

  p m

  correct_cities = {'Council Blfs' => 'Council Bluffs',
                    'Co. Bluffs'  => 'Council Bluffs',
                    'DSM'  => 'Des Moines',
                    'WDM'  => 'West Des Moines',
                    'SGT. Bluff' => 'Sergeant Bluff'}
  re = Regexp.union(correct_cities.keys)
  city = m[:city].gsub(re, correct_cities)
  city =
    if m[:city].length > 5
      MiniLokiC::DataMatching::NearestWord.correct_city_name(city, m[:state], 1)
    else
      MiniLokiC::DataMatching::NearestWord.shortening_fix(MiniLokiC::DataMatching::NearestWord.lstrip_nonalpha(city))
    end
  [city, m[:state].upcase, m[:zip][0..4]]
end

def check_unmatched(table_info, route)
  query = <<~SQL
    SELECT COUNT(*) AS total_unmatched
    FROM #{table_info[:clean_table]}
    WHERE pl_production_org_id IS NULL
      AND state = 'IA'
      AND committee_number IN
          (SELECT DISTINCT committee_code
           FROM IECDB_iowa_campaign_disclosures_contributions
           UNION
           SELECT DISTINCT committee_code
           FROM IECDB_iowa_campaign_disclosures_expenditures);
  SQL
  total_unmatched = route.query(query).to_a.first['total_unmatched']
  unless total_unmatched.zero?
    message_to_slack("#{total_unmatched} committees in *#{table_info[:clean_table]}* table should be matched.", :warning)
  end
end

############## CONTRIBUTORS & PAYEE CLEANING

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
    raw_column = table_info.key?(:raw_city) ? table_info[:raw_city] : table_info[:raw_column]
    clean_column = table_info.key?(:clean_city) ? table_info[:clean_city] : table_info[:clean_column]
    constraints = "UNIQUE (#{raw_column})"
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20)," : nil
    if table_info[:clean_state]
      state = "#{table_info[:clean_state]} VARCHAR(2),"
      usa_adcp_matching_id = "usa_adcp_matching_id INT DEFAULT NULL,"
      constraints = "CONSTRAINT city_state UNIQUE (#{table_info[:clean_state]}, #{table_info[:raw_city]})"
    else
      state = nil
      usa_adcp_matching_id = nil
    end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{raw_column} VARCHAR(255) NOT NULL,
         #{clean_column} VARCHAR(255) DEFAULT NULL,
         #{type}
         #{state}
         #{usa_adcp_matching_id}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         #{constraints})
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    #{local_connection}
    puts create_table.red
    route.query(create_table)
    message_to_slack("Table *#{table_info[:clean_table]}* created", :warning)
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  #{"AND DATE(r.created_at) >= '#{recent_date}'" if recent_date && !where_part}
  # SELECT r.#{table_info[:raw_column]}, MIN(last_scrape_date) AS scrape_date
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]}, MIN(DATE(r.updated_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND r.updated_at >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  unless names_list.empty?
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

def clean_any_names(table_info, route, person = true)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL
    LIMIT 10000;
  SQL
  # puts query.green
  cleaned = false

  until cleaned
    names_to_clean = route.query(query).to_a
    if names_to_clean.empty?
      cleaned = true
    else
      names_to_clean.each do |row|
        puts row
        clean_name = row
        result_name = row[table_info[:raw_column]].dup.gsub(160.chr("UTF-8")," ").squeeze(' ').gsub("\u0092", "'").gsub('’', "'").gsub(/[\u0093\u0094]/, '"').strip.sub(/\s+\./, '.').sub(/\.{2,}/, '.').sub(/,{2,}/, ',').gsub('&amp;', '&').sub(/,+$/, '')
        while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
          result_name = result_name[1..-1]
        end
        clean_name[table_info[:clean_column]] =
          if person
            clean_name_part(result_name, table_info[:clean_table])
          else
            MiniLokiC::Formatize::Cleaner.org_clean(result_name.match?(/[^\x00-\x7F]{2}/) ? fix_encode(clean_name[table_info[:raw_column]], result_name, table_info[:clean_table]) : result_name)
          end
        update_query = <<~SQL
          UPDATE #{table_info[:clean_table]}
          SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}'
          WHERE id=#{clean_name['id']};
        SQL
        # puts update_query.red
        route.query(update_query)
      end
    end
  end
end

def clean_name_part(name, table)
  clean_name = name.dup.gsub(/&/, ' and ').squeeze(' ').strip
  return '' if clean_name.downcase == 'n/a'

  clean_name = fix_encode(name, clean_name, table) if clean_name.match?(/[^\x00-\x7F]{2}/)

  name_parts = clean_name.split(/\b/)
  # name_parts = fix_encode(name, name_parts, table) if name.match?(/[^\x00-\x7F]{2}/)
  name_parts.collect! do |part|
    (part.match?(/^[a-z]$/i) || part.match?(/^M[rs]s?|[JDS]r$/i) ? "#{part}." : part).capitalize
  end
  clean_name = MiniLokiC::Formatize::Cleaner.mac_mc(name_parts.join.squeeze('.'))
  clean_name.gsub(/(?!^)\b(and|or)\b/i){|e| e.to_s.downcase }
  # clean_name.gsub(/\b(M[rs]s?|Dr)\b/) { |match| match.end_with?('.') ? match : "#{match}." }.gsub(/(?!^)\b(and|or)\b/i){|e| e.to_s.downcase }
end
