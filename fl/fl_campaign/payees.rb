# Creator: Alex Kuzmenko

def websites_cleaning(string)
  F::Cleaner.org_clean(string).sub(/\b\w+\. ?[a-z]{1,3}\b/i) { "'#{$&.sub(' ', '').downcase}'" }
end

def manual_preparings(string)
  if string.index(WEBSITES_REGEXP) && string[0] == '.'
    ar = string.split(', ').map(&:downcase)
    ar.unshift(ar.delete_at(1)).compact!
    ar.unshift("#{ar.shift(2).join('')}")
    ar.join(' ')
  elsif string[0] == '&'
    ar = string.split(', ')
    ar.unshift(ar.delete_at(1)).compact!
    ar.join(' ')
  else
    string
  end
end

def payees_query
  <<~SQL
    SELECT
      expnd.payee_name
    FROM fl_campaign_expenditure AS expnd
    LEFT JOIN fl_campaign_payee_names AS cln_payee
      ON cln_payee.payee_name = expnd.payee_name
    WHERE expnd.date >= '2021-01-01'
      AND expnd.type != 'REF'
      AND expnd.amount >= 0
      AND expnd.payee_name IS NOT NULL
      AND cln_payee.payee_name_clean IS NULL
      AND (cln_payee.bad_payee = FALSE OR cln_payee.bad_payee IS NULL)
    GROUP BY expnd.payee_name
    ORDER BY expnd.payee_name;
  SQL
end

def payees_cleaning
  begin
    db01 = C::Mysql.on(DB01, 'usa_raw')

    payees_to_clean = db01.query(payees_query).to_a.map { |r| r['payee_name'] }

    det = F::Determiner.new

    payees_to_clean.each do |payee|
      temp_payee = manual_preparings(payee)
      payee_type =
        if temp_payee.index(WEBSITES_REGEXP)
          'Website'
        else
          det.determine(temp_payee)
        end

      if payee_type == 'Person'
        temp_payee = F::Cleaner.person_clean(temp_payee, false)
        bad_result = 0
      elsif payee_type == 'Organization'
        temp_payee = F::Cleaner.org_clean(temp_payee)
        bad_result = 0
      elsif payee_type == 'Website'
        temp_payee = websites_cleaning(temp_payee)
        bad_result = 0
      else
        temp_payee = ''
        bad_result = 1
      end
      temp_payee.multi_gsub!('THE ', 'The ')
                .multi_gsub!(/U\.?s\.?p\.?s\.?/, 'USPS')
                .multi_gsub!('. Com', '.com')
                .multi_gsub!('Us ', 'US ')
                .multi_gsub!(' LLC.', ' LLC')
                .multi_gsub!(/ Inc[^\.]/, ' Inc.')
      payee_clean = temp_payee

      ins_query = <<~SQL
        INSERT INTO fl_campaign_payee_names(payee_name, payee_name_clean, payee_type, bad_payee)
        VALUES(#{payee.dump}, #{payee_clean.dump}, #{payee_type.dump}, #{bad_result});
      SQL
      db01.query(ins_query)
    end
  rescue Mysql2::Error => e
    puts "!!\nMysql2 Exception\n!!"
    p e
  rescue Exception => e
    puts "!!\nCommon Exception\n!!"
    p e
  ensure
    db01&.close
  end
  puts 'Payees - Done'
end
