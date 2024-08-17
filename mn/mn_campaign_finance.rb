# Creator:      Sergii Butrymenko
# Dataset Name: Minnesota Campaign Finance
# Task #:       99
# Migrated:     June 2023

# ruby mlc.rb --tool="clean::mn::mn_campaign_finance"
# ruby mlc.rb --tool="clean::mn::mn_campaign_finance" --mode='committee'
# ruby mlc.rb --tool="clean::mn::mn_campaign_finance" --mode='candidate'
# ruby mlc.rb --tool="clean::mn::mn_campaign_finance" --mode='contributor'
# ruby mlc.rb --tool="clean::mn::mn_campaign_finance" --mode='vendor'
# ruby mlc.rb --tool="clean::mn::mn_campaign_finance" --mode='purpose'


def execute(options = {})
  route = C::Mysql.on(DB01, 'usa_fin_cc_raw')
  table_description = {
    committee: {
      raw_table: 'minnesota_campaign_finance_committees',
      clean_table: 'minnesota_campaign_finance_committees__matched',
    },
    candidate: {
      raw_table: 'minnesota_campaign_finance_candidates',
      clean_table: 'minnesota_campaign_finance_candidates__clean',
    },
    party: {
      raw_table: 'minnesota_campaign_finance_parties',
      clean_table: 'minnesota_campaign_finance_parties__clean',
    },
    contributor: {
      raw_table: 'minnesota_campaign_finance_contributions_csv',
      clean_table: 'minnesota_campaign_finance__contributors_clean',
      raw_column: 'contributor_full_name',
      clean_column: 'contributor_full_name_clean',
      fn_column: 'contributor_first_name',
      mn_column: 'contributor_middle_name',
      ln_column: 'contributor_last_name',
      sf_column: 'contributor_suffix',
      raw_type_column: 'contributor_type',
      type_column: 'name_type'
    },
    vendor: {
      raw_table: 'minnesota_campaign_finance_expenditures_csv',
      clean_table: 'minnesota_campaign_finance__vendors_clean',
      raw_column: 'vendor_name',
      clean_column: 'vendor_name_clean',
      type_column: 'name_type'
    },
    purpose: {
      raw_table: 'minnesota_campaign_finance_expenditures_csv',
      clean_table: 'minnesota_campaign_finance__purpose_clean',
      raw_column: 'purpose',
      clean_column: 'purpose_clean',
    }
  }
  where_part = options['where']
  mode = options['mode']&.to_sym
  table_info = table_description[mode]
  case mode
  when :committee
    clean_committee_info(table_info, route)
    # check_unmatched(table_info, route)
  when :candidate
    clean_candidate_info(table_info, route)
    check_unmatched(table_info, route)
    # uniq_committee(table_info, route)
    # recent_date = get_recent_date(table_info, route)
    # fill_table(table_info, recent_date, where_part, route)
    # clean_ind_names(table_info, route)
  when :contributor
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
    # split_names(table_info, route)
  when :vendor
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_mixed_names(table_info, route)
  when :purpose
    recent_date = get_recent_date(table_info, route)
    fill_table(table_info, recent_date, where_part, route)
    clean_expenditures(table_info, route)
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
    text: "*[CLEANING #99] ST 338 Minnesota Campaign Finance* \n>#{type} #{message}",
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

# Committee Cleaning

def clean_committee_info(table_info, route)
  committee_data = get_committee_data(table_info, route)
  committee_data.each do |item|
    puts JSON.pretty_generate(item).yellow
    committee_name = clean_committee_name(item[:committee_name])
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:clean_table]}
      (committee_id, committee_name, committee_name_clean, committee_address1, committee_address2, committee_city, committee_state, committee_zip, scrape_date, created_by)
      VALUES
      ('#{item[:committee_id]}', #{escape_or_null(item[:committee_name])}, #{escape_or_null(committee_name)}, #{escape_or_null(item[:committee_address1])}, #{escape_or_null(item[:committee_address2])}, #{escape_or_null(item[:committee_city])}, #{escape_or_null(item[:committee_state])}, #{escape_or_null(item[:committee_zip])}, #{escape_or_null(item[:scrape_date])}, 'Sergii Butrymenko');
    SQL
    # puts insert_query
    route.query(insert_query)
  end
end

def get_committee_data(table_info, route)
  query = <<~SQL
    SELECT s.registered_entity_id AS committee_id,
           s.formatted_name AS committee_name,
           s.address1 AS committee_address1,
           s.address2 AS committee_address2,
           s.city AS committee_city,
           s.state AS committee_state,
           s.zip_code AS committee_zip,
           DATE(s.created_at) AS scrape_date
    FROM #{table_info[:raw_table]} s
      INNER JOIN (SELECT registered_entity_id, MAX(id) AS max_id
                  FROM #{table_info[:raw_table]}
                  WHERE role_type='Committee'
                  GROUP BY registered_entity_id) mx
        ON s.id = mx.max_id
      LEFT JOIN #{table_info[:clean_table]} m
        ON s.registered_entity_id = m.committee_id
    WHERE m.id IS NULL
      AND s.role_type='Committee';
  SQL
  puts query.green
  retry_count = 0
  begin
    committee_data = route.query(query, symbolize_keys: true).to_a
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    create_query = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} (
          id bigint(20) AUTO_INCREMENT PRIMARY KEY,
          committee_id VARCHAR(255) NOT NULL,
          committee_name VARCHAR(255) NOT NULL,
          committee_name_clean VARCHAR(255),
          committee_address1 VARCHAR(255),
          committee_address2 VARCHAR(255),
          committee_city VARCHAR(255),
          committee_state VARCHAR(255),
          committee_zip VARCHAR(255),
          fixed_manually BOOLEAN NOT NULL DEFAULT 0,
          skip_it BOOLEAN NOT NULL DEFAULT 0,
          scrape_date DATE NOT NULL DEFAULT '0000-00-00',
          created_by VARCHAR(255) NOT NULL,
          created_at timestamp DEFAULT CURRENT_TIMESTAMP,
          updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX committee__idx (committee_id, committee_name))
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
  result_name = committee_name.dup.strip
  while (result_name.start_with?('"') && result_name.end_with?('"')) || result_name.start_with?("'") && result_name.end_with?("'")
    result_name = result_name[1...-1]
  end
  result_name.sub!(%r(\sC/O.+)i, '')
  result_name.gsub!('&amp;', '&')
  result_name.gsub!('&#39;', "'")
  result_name.gsub!(/\bCOMM?\b\.?/i, 'Committee')
  result_name.gsub!(/\bM(in)?n\b\.?/i, 'Minnesota')
  result_name.gsub!(/\bST(ATE)? REP\b\.?/i, 'State Representative')
  result_name.gsub!(/\bDEM\b\.?/i, 'Democratic')
  result_name.gsub!(/\bREP\b\.?/i, 'Republican')
  result_name.gsub!(/\bCO\b\.?(?!-)/i, 'County')
  result_name.gsub!(/\bEMPL\b/i, 'Employee')
  result_name.gsub!(/\bEduc\b/i, 'Education')
  result_name.gsub!(/\bEMPLS\b/i, 'Employees')
  result_name.gsub!(/\bFn?d\b/i, 'Fund')
  result_name.gsub!(/\bGOV\b/i, 'Government')
  result_name.gsub!(/\bLEG\b/i, 'Legislative')
  result_name.gsub!(/\bCAMP\b/i, 'Campaign')
  result_name.gsub!(/\bTWP\b/i, 'Township')
  result_name.gsub!(/\bPOL\b/i, 'Political')
  result_name.gsub!(/\bFED\b/i, 'Federation')
  result_name.gsub!(/\bWRKRS\b/i, 'Workers')

  result_name = MiniLokiC::Formatize::Cleaner.org_clean(result_name.strip.squeeze(' '))

  if result_name.match?(/(FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)$/i)
    m = result_name.match(/(.+) (FAMILY AND FRIENDS OF|FRIENDS OF|FRIENDS FOR|THE COMMITTEE TO ELECT|COMMITTEE TO ELECT|CITIZENS FOR|FRIENDS OF SENATOR|WE BELIEVE IN|TAXPAYERS FOR|VOLUNTEERS FOR|PEOPLE FOR)/i).captures
    result_name =
      if m[0].match?(/,/)
        m[1] + ' ' + m[0].match(/(.+), ?(.*)/).captures.reverse.join(' ')
      else
        m.join(' ')
      end
  end
  result_name
end

def check_unmatched(table_info, route)
  query = <<~SQL
    SELECT COUNT(*) AS total_unmatched
    FROM #{table_info[:clean_table]}
    WHERE committee_state = 'MN'
      AND pl_production_org_id IS NULL
      AND committee_city IS NOT NULL
      AND committee_city <> ''
      AND committee_id IN (
        SELECT DISTINCT site_source_committee_id
        FROM minnesota_campaign_finance_contributions_csv
        UNION
        SELECT DISTINCT committee_id
        FROM pa_campaign_finance_expenditures_new_csv);
  SQL
  total_unmatched = route.query(query).to_a.first['total_unmatched']
  unless total_unmatched.zero?
    message_to_slack("#{total_unmatched} committees in *#{table_info[:clean_table]}* table should be matched.", :warning)
  end
end

# Candidate Cleaning

def clean_candidate_info(table_info, route)
  candidate_data = get_candidate_data(table_info, route)
  candidate_data.each do |item|
    puts JSON.pretty_generate(item).yellow
    committee_name = clean_committee_name(item[:committee_name])
    insert_query = <<~SQL
      INSERT IGNORE INTO #{table_info[:clean_table]}
      (committee_id, committee_name, committee_name_clean, committee_address1, committee_address2, committee_city, committee_state, committee_zip, scrape_date, created_by)
      VALUES
      ('#{item[:committee_id]}', #{escape_or_null(item[:committee_name])}, #{escape_or_null(committee_name)}, #{escape_or_null(item[:committee_address1])}, #{escape_or_null(item[:committee_address2])}, #{escape_or_null(item[:committee_city])}, #{escape_or_null(item[:committee_state])}, #{escape_or_null(item[:committee_zip])}, #{escape_or_null(item[:scrape_date])}, 'Sergii Butrymenko');
    SQL
    # puts insert_query
    route.query(insert_query)
  end
end

def get_candidatee_data(table_info, route)
  query = <<~SQL
    SELECT s.registered_entity_id AS committee_id,
           s.formatted_name AS committee_name,
           s.address1 AS committee_address1,
           s.address2 AS committee_address2,
           s.city AS committee_city,
           s.state AS committee_state,
           s.zip_code AS committee_zip,
           DATE(s.created_at) AS scrape_date
    FROM #{table_info[:raw_table]} s
      INNER JOIN (SELECT registered_entity_id, MAX(id) AS max_id
                  FROM #{table_info[:raw_table]}
                  WHERE role_type='Committee'
                  GROUP BY registered_entity_id) mx
        ON s.id = mx.max_id
      LEFT JOIN #{table_info[:clean_table]} m
        ON s.registered_entity_id = m.committee_id
    WHERE m.id IS NULL
      AND s.role_type='Committee';
  SQL
  puts query.green
  retry_count = 0
  begin
    committee_data = route.query(query, symbolize_keys: true).to_a
  rescue Mysql2::Error
    message_to_slack("Clean table *#{table_info[:clean_table]}* doesn't exist. Creating it now...", :warning)
    create_query = <<~SQL
      CREATE TABLE #{table_info[:clean_table]} (
          id bigint(20) AUTO_INCREMENT PRIMARY KEY,
          committee_id VARCHAR(255) NOT NULL,
          committee_name VARCHAR(255) NOT NULL,
          committee_name_clean VARCHAR(255),
          committee_address1 VARCHAR(255),
          committee_address2 VARCHAR(255),
          committee_city VARCHAR(255),
          committee_state VARCHAR(255),
          committee_zip VARCHAR(255),
          fixed_manually BOOLEAN NOT NULL DEFAULT 0,
          skip_it BOOLEAN NOT NULL DEFAULT 0,
          scrape_date DATE NOT NULL DEFAULT '0000-00-00',
          created_by VARCHAR(255) NOT NULL,
          created_at timestamp DEFAULT CURRENT_TIMESTAMP,
          updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
          INDEX committee__idx (committee_id, committee_name))
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
    type = table_info[:type_column] ? "#{table_info[:type_column]} VARCHAR(20) DEFAULT NULL," : nil
    name_parts =
      if table_info[:fn_column]
        <<~MySQL
        #{table_info[:fn_column]} VARCHAR(255) DEFAULT NULL,
        #{table_info[:mn_column]} VARCHAR(255) DEFAULT NULL,
        #{table_info[:ln_column]} VARCHAR(255) DEFAULT NULL,
        #{table_info[:sf_column]} VARCHAR(255) DEFAULT NULL,
        MySQL
      else
        nil
      end
    constraints = if table_info[:raw_type_column]
                    "#{table_info[:raw_column]}, #{table_info[:raw_type_column]}"
                  else
                    "#{table_info[:raw_column]}"
                  end
    create_table = <<~SQL
      CREATE TABLE #{table_info[:clean_table]}
        (id bigint(20) AUTO_INCREMENT PRIMARY KEY,
         #{table_info[:raw_column]} VARCHAR(255) NOT NULL,
         #{"#{table_info[:raw_type_column]} VARCHAR(255) NOT NULL," if table_info[:raw_type_column]}
         #{table_info[:clean_column]} VARCHAR(255) DEFAULT NULL,
         #{name_parts}
         #{type}
         fixed_manually BOOLEAN NOT NULL DEFAULT 0,
         skip_it BOOLEAN NOT NULL DEFAULT 0,
         scrape_date DATE NOT NULL DEFAULT '0000-00-00',
         cleaning_dev_name VARCHAR(255) NOT NULL,
         created_at timestamp DEFAULT CURRENT_TIMESTAMP,
         updated_at timestamp DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
         UNIQUE (#{constraints}))
      CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_520_ci;
    SQL
    puts create_table.red
    route.query(create_table)
    message_to_slack("Table *#{table_info[:clean_table]}* created", :warning)
    recent_date = nil
  end
  puts "Clean table RECENT DATE: #{recent_date}"
  recent_date
end

def fill_table(table_info, recent_date, where_part, route)
  query = <<~SQL
    SELECT r.#{table_info[:raw_column]},
           #{"r.#{table_info[:raw_type_column]}," if table_info[:raw_type_column]}
           MIN(DATE(r.created_at)) AS scrape_date
    FROM #{table_info[:raw_table]} r
      LEFT JOIN #{table_info[:clean_table]} cl
        ON r.#{table_info[:raw_column]} = cl.#{table_info[:raw_column]}
          #{"AND (r.#{table_info[:raw_type_column]} = cl.#{table_info[:raw_type_column]} OR r.#{table_info[:raw_type_column]} IS NULL AND cl.#{table_info[:raw_type_column]} IS NULL)"  if table_info[:raw_type_column]}
    WHERE cl.#{table_info[:raw_column]} IS NULL
      AND r.#{table_info[:raw_column]} IS NOT NULL
      #{"AND r.created_at >= '#{recent_date}'" if recent_date && !where_part}
      #{"AND #{where_part}" if where_part}
    GROUP BY r.#{table_info[:raw_column]}#{", r.#{table_info[:raw_type_column]}" if table_info[:raw_type_column]};
  SQL
  puts query.green
  names_list = route.query(query).to_a
  unless names_list.empty?
    parts = names_list.each_slice(10_000).to_a
    parts.each do |part|
      insert_query = <<~SQL
        INSERT IGNORE INTO #{table_info[:clean_table]} (#{table_info[:raw_column]}#{", #{table_info[:raw_type_column]}" if table_info[:raw_type_column]}, scrape_date, cleaning_dev_name)
        VALUES
      SQL
      part.each do |item|
        next if item[table_info[:raw_column]].nil?

        scrape_date = item['scrape_date'].nil? ? 'NULL' : "'#{item['scrape_date']}'"
        insert_query << "('#{escape(item[table_info[:raw_column]])}'#{", #{escape_or_null(item[table_info[:raw_type_column]])}" if table_info[:raw_type_column]}, #{scrape_date}, 'Sergii Butrymenko'),"
      end
      insert_query = "#{insert_query.chop};"
      puts insert_query.red
      route.query(insert_query)
    end
  end
end

def clean_mixed_names(table_info, route)
  query = <<~SQL
    SELECT id, #{table_info[:raw_column]}
      #{", #{table_info[:raw_type_column]}" if table_info[:raw_type_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL
    LIMIT 10000;
  SQL
  # puts query.green
  cleaned = false

  det = MiniLokiC::Formatize::Determiner.new

  ns = init_name_splitter(table_info)

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
        if %w[Individual Self].include?(clean_name[table_info[:raw_type_column]])
          clean_name[table_info[:clean_column]] = MiniLokiC::Formatize::Cleaner.person_clean(result_name)
          clean_name[table_info[:type_column]] = 'Person'
        elsif ['Candidate Committee', 'Party Unit', 'Political Committee/Fund'].include?(clean_name[table_info[:raw_type_column]])
          clean_name[table_info[:clean_column]] = MiniLokiC::Formatize::Cleaner.org_clean(result_name)
          clean_name[table_info[:type_column]] = 'Organization'
        else
          clean_name[table_info[:type_column]] = det.determine(result_name)
          clean_name[table_info[:clean_column]] =
          if clean_name[table_info[:type_column]] == 'Person'
            MiniLokiC::Formatize::Cleaner.person_clean(result_name)
          else
            MiniLokiC::Formatize::Cleaner.org_clean(result_name)
          end
        end
        splitted_name = {}
        if clean_name[table_info[:type_column]] == 'Person' && !clean_name[table_info[:clean_column]].empty?
          splitted_name, _action = ns.split_record('full_name' => clean_name[table_info[:clean_column]])
        end

        update_query = <<~SQL
          UPDATE #{table_info[:clean_table]}
          SET #{table_info[:clean_column]}='#{escape(clean_name[table_info[:clean_column]])}',
            #{table_info[:type_column]}='#{escape(clean_name[table_info[:type_column]])}',
            #{table_info[:fn_column]} = '#{escape(splitted_name[:first_name])}',
            #{table_info[:mn_column]} = '#{escape(splitted_name[:middle_name])}',
            #{table_info[:ln_column]} = '#{escape(splitted_name[:last_name])}',
            #{table_info[:sf_column]} = '#{escape(splitted_name[:suffix])}'
          WHERE id=#{clean_name['id']};
        SQL
        # puts update_query.red
        route.query(update_query)
      end
    end
  end
end

def init_name_splitter(table_info)
  require_relative '../../namesplit/namesplit'
  ns_options = {
    'host' => DB01,
    'database' => 'usa_fin_cc_raw',
    'table' => table_info[:clean_table],
    'no-business' => 'enabled',
    'no-double_lastname' => 'enabled',
    'mode' => 'split',
    'field' => table_info[:clean_column],
    'style' => 'fml',
    'no_print_results' => 'enabled'
  }

  NameSplit.new(ns_options)
end

# Clean expenditures

def clean_expenditures(table_info, route)
  query = <<~SQL
    SELECT #{table_info[:raw_column]}
    FROM #{table_info[:clean_table]}
    WHERE #{table_info[:clean_column]} IS NULL;
  SQL
  puts query.green
  items_list = route.query(query).to_a

  return if items_list.empty?

  items_list.each do |row|
    puts row
    purpose = row[table_info[:raw_column]].dup
    puts purpose

    if (purpose.size <= 2 && purpose.downcase != 'ad' && !purpose.empty?) || !purpose.match?(/[a-z]/i)
      clean_purpose = 'Unspecified Expenses'
    else
      clean_purpose =
        case purpose
        when nil
          'Unspecified Expenses'
        when ''
          'Unspecified Expenses'
        when /\berrors?\b/i
          'Improper Payments'
        when /\b(ads?|advertising|banner|billboards?|books?|brochures|digital|lawn signs|masks|parade|posters?|promotional|t-shirts|tv|yard signs)\b/i
          'Advertisement'
        when /\b(auto|car|fuel|mileage|parking|taxi)\b/i
          'Vehicle Expenses'
        when /\b(cleaning|window)\b/i
          'Cleaning Services'
        when /\bcampaign\b/i
          'Campaign Expenses'
        when /\b(advocacy|attorney|legal|notary)\b/i
          'Legal Fees & Expenses'
        when /\b(air fare|hotel|lodging|travel)\b/i
          'Travel Expenses'
        when /\b(auction|raffle)\b/i
          'Auction Items & Supplies'
        when /\bbank\b/i
          'Financial Expenses'
        when /\b(donation|dues)\b/i
          'Donations'
        when /\belection\b/i
          'Election Expenses'
        when /\belectric(ity)?\b/i
          'Electric Service'
        when /\bequipment\b/i
          'Equipment, Rental'
        when /\b(christmas|dining|dinner|dj|event|gala|holiday|tickets|tix|xmas)\b/i
          'Event Expenses'
        when /\bfund(raiser|raising)?\b/i
          'Fundraising Expenses'
        when /\bgas(oline)?\b/i
          'Gasoline Expenses'
        when /\bgifts?\b/i
          'Gifts'
        when /\bgolf\b/i
          'Golf Outing Expenses'
        when /\bhealth\b/i
          'Health Insurance Expense'
        when /\b(Act\s?Blue|cable|calls|cell|communications|copie[rs]|copy|data(base)?|domain|graphic|ink|intern( services)?|internet|messenger|office|organiz(ation|er)|pens|printing|software|supplies|support|tech(nical|nology)?|(tele)?phone|web)\b/i
          'Technology & Office Expenses'
        when /IT/
          'Technology & Office Expenses'
        when /\b(media|telecom(munications)?|teleconference|video)\b/i
          'Media Production'
        when /\b(breakfast|catering|drinks|entertainment|flowers|food|lunch|meals|pizza|refreshments)\b/i
          'Meals & Entertainment'
        when /\b(marketing|pr)\b/i
          'Marketing'
        when /\b(e?mail(ing)?|envelopes|post(age)?|shipping|stamps)\b/i
          'Mail & Postage'
        when /\bmeeting\b/i
          'Meeting Expenses'
        when /\bmembership\b/i
          'Membership Dues'
        when /\bmerchan(dise|t)\b/i
          'Merchant Expenses'
        when /\bpayroll\b/i
          'Payroll Expenses'
        when /\b(photo(copies|graphs|graphy|s)?)\b/i
          'Media Production'
        when /\bprincipal\b/i
          'Principal Payment'
        when /\bstaff\b/i
          'Staff'
        when /\b(rent|utilities)\b/i
          'Rent & Utilities'
        when /\b(card|credit)\b/i
          'Credit Card Fees & Expenses'
        when /\bcontribut(e|ion)\b/i
          'Contribution'
        when /\bconsult(ant|ing)\b/i
          'Consulting'
        when /\btax(es)?\b/i
          'Taxes'
        when /\bfield\b/i
          'Field Work'
        when /\binsurance\b/i
          'Insurance'
        when /\b(collection|conference|e-?commerce|lobbyist|registration|transaction|wire transfer|witness)\b/i
          'Fees'
        when /\bgeneral\b/i
          'General Contribution'
        when /\bprimary\b/i
          'Primary Contribution'
        else
          'Other Expenses'
        end
    end
    update_query = <<~SQL
      UPDATE #{table_info[:clean_table]}
      SET #{table_info[:clean_column]}='#{escape(clean_purpose)}'
      WHERE #{table_info[:clean_column]} IS NULL
        AND #{table_info[:raw_column]}='#{escape(row[table_info[:raw_column]])}';
    SQL
    puts update_query.red
    route.query(update_query)
  end
end

# def split_names(table_info, route)
#   query = <<~SQL
#     SELECT id, #{table_info[:raw_column]}, #{table_info[:clean_column]}
#     FROM #{table_info[:clean_table]}
#     WHERE #{table_info[:ln_column]} IS NULL
#       AND #{table_info[:type_column]}='Person';
#   SQL
#   names_list = route.query(query).to_a
#   return if names_list.empty?
#
#   ns = init_name_splitter
#   names_list.each do |row|
#     clean_name = row
#     result_name = row[table_info[:raw_column]].dup
#     splitted_name, _action = ns.split_record('full_name' => row[table_info[:raw_column]])
#
#     puts "CLEANER: #{cleaner_name} <> #{}"
#
#     clean_name[table_info[:clean_column]] = result_name
#     update_query = <<~SQL
#       UPDATE #{table_info[:clean_table]}
#       SET #{table_info[:clean_column]}='#{escape(splitted_name[:full_name])}',
#           #{table_info[:fn_table]} = '#{escape(splitted_name[:first_name])}',
#           #{table_info[:mn_table]} = '#{escape(splitted_name[:middle_name])}',
#           #{table_info[:ln_table]} = '#{escape(splitted_name[:last_name])}',
#           #{table_info[:sf_table]} = '#{escape(splitted_name[:suffix])}'
#       WHERE id=#{clean_name['id']}
#         AND #{table_info[:ln_column]} IS NULL
#         AND #{table_info[:raw_column]}='#{escape(clean_name[table_info[:raw_column]])}';
#     SQL
#     puts update_query
#     # route.query(update_query)
#   end
# end
#
#
# def init_name_splitter
#   require_relative '../../namesplit/namesplit'
#   ns_options = {
#     'host' => DB01,
#     'database' => 'usa_fin_cc_raw',
#     'table' => 'minnesota_campaign_finance__contributors_clean',
#     'no-business' => 'enabled',
#     'no-double_lastname' => 'enabled',
#     'style' => 'fml',
#     'mode' => 'split',
#     'field' => 'name',
#     'no_print_results' => 'enabled'
#   }
#
#   NameSplit.new(ns_options)
# end